use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::{CockatielClient, PromptKind};

// GUILD_MESSAGES (1<<9) + MESSAGE_CONTENT (1<<15). MESSAGE_CONTENT is a
// privileged intent — enable it in the Discord Developer Portal for the bot.
const DISCORD_INTENTS: u32 = (1 << 9) | (1 << 15);
const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";
const REST_API: &str = "https://discord.com/api/v10";
/// Discord requires a User-Agent on every API request.
const USER_AGENT: &str = "cockatiel-discord-adapter/0.1.0";
/// Per-request ceiling for a `GET /users/@me` token check, independent of the
/// client-wide timeout, so a hung socket can't wedge the setup phase.
const AUTH_CHECK_TIMEOUT_SECS: u32 = 10;
/// Consecutive rejections tolerated for ONE token before the setup phase gives
/// up and lets the supervisor restart the module. A newly submitted token gets
/// a full budget (see [`should_reset_rejection_budget`]); re-submitting the
/// token that was just rejected does not, so pasting the same wrong token
/// three times ends the setup instead of looping forever.
const MAX_AUTH_ATTEMPTS: u32 = 3;
/// Hard ceiling on how many tokens the operator may submit in a single setup
/// run. Each distinct token earns a fresh [`MAX_AUTH_ATTEMPTS`] budget, so
/// without this ceiling a stream of different bad tokens would prompt forever.
const MAX_AUTH_SUBMISSIONS: u32 = 10;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

/// The module's engine-session identity (auth token + assigned instance + name).
/// Held in a shared Mutex so a reconnect can swap it in place and every other
/// task (read loop, Discord platform send path) always uses the CURRENT
/// session's credentials — a stale token after a reconnect would be rejected
/// by the engine and the module would look dead.
#[derive(Clone, Default)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

/// A queued Discord platform send. Outbound REST calls are handed to a bounded
/// worker pool so the engine read loop never blocks on a slow Discord send.
struct DiscordSendJob {
    client: reqwest::Client,
    token: String,
    channel_id: String,
    msg: String,
    embed: bool,
    http_timeout_secs: u32,
}

/// Re-register the adapter's chat commands with the engine (called on the
/// initial connect AND after every reconnect — the engine forgets a session's
/// commands when the socket drops).
async fn register_commands(
    write: &Arc<Mutex<WsWriteHalf>>,
    identity: &Arc<Mutex<EngineIdentity>>,
) {
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let commands = ContainerForEngine {
        version: 2,
        auth_token: auth.clone(),
        module_name: module.clone(),
        module_instance_uuid7: instance.clone(),
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![
                Command {
                    command_name: "ban".to_string(),
                    command_flag: "!".to_string(),
                    command_description: "ban a user".to_string(),
                    command_flags: vec![],
                },
                Command {
                    command_name: "timeout".to_string(),
                    command_flag: "!".to_string(),
                    command_description: "timeout a user".to_string(),
                    command_flags: vec![],
                },
            ],
            alert_on_unknown_command: false,
        })),
    };
    let mut cbuf = Vec::new();
    if commands.encode(&mut cbuf).is_ok() {
        let mut w = write.lock().await;
        let _ = w.send(WsMessage::Binary(cbuf)).await;
    }
    info!("registered !ban / !timeout commands");
}

/// Channel policy for one server.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ServerChannels {
    /// Monitor/send to every channel in the server (["*"]).
    All,
    /// Monitor/send to a specific set of channels.
    Some(Vec<String>),
}

/// Parse `module_specific.servers` (`{ "guild_id": ["ch1","ch2"] | ["*"] }`)
/// into a server → policy map. An empty list or a ["*"] list means all channels.
fn parse_servers(
    servers: &std::collections::HashMap<String, Vec<String>>,
) -> std::collections::HashMap<String, ServerChannels> {
    let mut out = std::collections::HashMap::new();
    for (guild, channels) in servers {
        let policy = if channels.is_empty() || channels.iter().any(|c| c == "*") {
            ServerChannels::All
        } else {
            ServerChannels::Some(channels.clone())
        };
        out.insert(guild.clone(), policy);
    }
    out
}

/// Parse one `guild=ch1,ch2` line into a server → policy entry. A bare
/// `guild` (no `=`) means every channel in that server.
fn parse_server_line(line: &str) -> Option<(String, Vec<String>)> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    let (guild, channels) = match line.split_once('=') {
        Some((g, c)) => (g.trim(), c.trim()),
        None => (line, ""),
    };
    if guild.is_empty() {
        return None;
    }
    let channels: Vec<String> = channels
        .split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect();
    Some((guild.to_string(), channels))
}

/// Read `module_specific.servers` in ANY of the shapes it can legitimately
/// arrive in, so this module and the engine's credential form stop
/// clobbering each other.
///
/// Two writers own this key and they use different datatypes:
///   - the module's own `save_adapter_config` writes the OBJECT form
///     `{ "guild_id": ["ch1","ch2"] }`;
///   - the engine's `save_module_credentials` writes a `list: true` field as
///     an ARRAY of newline-split strings `["guild_id=ch1,ch2"]`.
///
/// Deserialising the array form straight into `HashMap<String, Vec<String>>`
/// fails with "invalid type: sequence, expected a map", and the old code
/// swallowed that with `.ok()`, so saving credentials in the TUI silently
/// emptied the server list. Both forms are accepted here.
fn parse_servers_value(v: &serde_json::Value) -> std::collections::HashMap<String, Vec<String>> {
    let mut out: std::collections::HashMap<String, Vec<String>> = Default::default();
    match v {
        // Canonical form: { "guild_id": ["ch1","ch2"] | ["*"] }
        serde_json::Value::Object(map) => {
            for (guild, channels) in map {
                let list: Vec<String> = match channels {
                    serde_json::Value::Array(items) => items
                        .iter()
                        .filter_map(|i| i.as_str().map(|s| s.trim().to_string()))
                        .filter(|s| !s.is_empty())
                        .collect(),
                    serde_json::Value::String(s) => parse_server_line(s)
                        .map(|(_, chs)| chs)
                        .unwrap_or_default(),
                    _ => Vec::new(),
                };
                out.insert(guild.trim().to_string(), list);
            }
        }
        // Engine form: ["guild_id=ch1,ch2", "guild2", ...]
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(line) = item.as_str() {
                    if let Some((guild, channels)) = parse_server_line(line) {
                        out.insert(guild, channels);
                    }
                }
            }
        }
        // A single hand-edited line.
        serde_json::Value::String(s) => {
            for line in s.lines() {
                if let Some((guild, channels)) = parse_server_line(line) {
                    out.insert(guild, channels);
                }
            }
        }
        _ => {}
    }
    out
}

/// Serialize a server → policy map back into the `servers` object form.
fn serialize_servers(servers: &std::collections::HashMap<String, ServerChannels>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    let mut keys: Vec<&String> = servers.keys().collect();
    keys.sort();
    for guild in keys {
        let chs = match servers[guild] {
            ServerChannels::All => vec!["*".to_string()],
            ServerChannels::Some(ref list) => list.clone(),
        };
        obj.insert(
            guild.clone(),
            serde_json::Value::Array(chs.into_iter().map(serde_json::Value::String).collect()),
        );
    }
    serde_json::Value::Object(obj)
}

/// Union channel IDs across every server, deduped + sorted. `Some` policies
/// contribute their explicit list; `All` ("*") policies contribute the
/// caller-resolved text-channel list for that guild (never silently dropped).
fn merge_send_channels(
    servers: &std::collections::HashMap<String, ServerChannels>,
    all_channels: &std::collections::HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (guild, policy) in servers {
        match policy {
            ServerChannels::Some(chs) => out.extend(chs.iter().cloned()),
            ServerChannels::All => out.extend(all_channels.get(guild).into_iter().flatten().cloned()),
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Resolve the full send-target list for SendToPlatforms. A `*` server is
/// resolved to the text channels the bot can see in that guild (via REST), so
/// engine replies are never silently dropped for wildcard-monitored servers.
async fn resolve_send_channels(
    client: &reqwest::Client,
    token: &str,
    servers: &std::collections::HashMap<String, ServerChannels>,
) -> Vec<String> {
    let mut all_channels = std::collections::HashMap::new();
    for (guild, policy) in servers {
        if matches!(policy, ServerChannels::All) {
            all_channels.insert(guild.clone(), guild_text_channels(client, token, guild).await);
        }
    }
    merge_send_channels(servers, &all_channels)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DiscordAdapterConfig {
    bot_token: Option<String>,
    #[serde(default)]
    servers: std::collections::HashMap<String, Vec<String>>,
    #[serde(default)]
    embed_sends: bool,
    // Runtime tuning knobs. Each `#[serde(default = ...)]` supplies the
    // previous hardcoded constant when the key is missing from config.json,
    // so an absent setting behaves exactly as before.
    #[serde(default = "DiscordAdapterConfig::default_timeout_secs")]
    default_timeout_secs: i32,
    #[serde(default = "DiscordAdapterConfig::http_timeout_secs")]
    http_timeout_secs: u32,
    #[serde(default = "DiscordAdapterConfig::gateway_auth_backoff_base_secs")]
    gateway_auth_backoff_base_secs: u32,
    #[serde(default = "DiscordAdapterConfig::gateway_auth_backoff_max_secs")]
    gateway_auth_backoff_max_secs: u32,
    #[serde(default = "DiscordAdapterConfig::gateway_connect_retry_secs")]
    gateway_connect_retry_secs: u32,
    #[serde(default = "DiscordAdapterConfig::gateway_reconnect_delay_secs")]
    gateway_reconnect_delay_secs: u32,
    #[serde(default = "DiscordAdapterConfig::outbound_queue_cap")]
    outbound_queue_cap: usize,
    #[serde(default = "DiscordAdapterConfig::send_worker_count")]
    send_worker_count: usize,
    #[serde(default = "DiscordAdapterConfig::reconnect_base_secs")]
    reconnect_base_secs: u32,
    #[serde(default = "DiscordAdapterConfig::reconnect_max_secs")]
    reconnect_max_secs: u32,
    #[serde(default = "DiscordAdapterConfig::prompt_timeout_secs")]
    prompt_timeout_secs: u32,
}

impl DiscordAdapterConfig {
    fn default_timeout_secs() -> i32 {
        300
    }
    fn http_timeout_secs() -> u32 {
        15
    }
    fn gateway_auth_backoff_base_secs() -> u32 {
        1
    }
    fn gateway_auth_backoff_max_secs() -> u32 {
        30
    }
    fn gateway_connect_retry_secs() -> u32 {
        5
    }
    fn gateway_reconnect_delay_secs() -> u32 {
        5
    }
    fn outbound_queue_cap() -> usize {
        64
    }
    fn send_worker_count() -> usize {
        4
    }
    fn reconnect_base_secs() -> u32 {
        1
    }
    fn reconnect_max_secs() -> u32 {
        30
    }
    fn prompt_timeout_secs() -> u32 {
        300
    }
}

impl Default for DiscordAdapterConfig {
    fn default() -> Self {
        Self {
            bot_token: None,
            servers: Default::default(),
            embed_sends: false,
            default_timeout_secs: Self::default_timeout_secs(),
            http_timeout_secs: Self::http_timeout_secs(),
            gateway_auth_backoff_base_secs: Self::gateway_auth_backoff_base_secs(),
            gateway_auth_backoff_max_secs: Self::gateway_auth_backoff_max_secs(),
            gateway_connect_retry_secs: Self::gateway_connect_retry_secs(),
            gateway_reconnect_delay_secs: Self::gateway_reconnect_delay_secs(),
            outbound_queue_cap: Self::outbound_queue_cap(),
            send_worker_count: Self::send_worker_count(),
            reconnect_base_secs: Self::reconnect_base_secs(),
            reconnect_max_secs: Self::reconnect_max_secs(),
            prompt_timeout_secs: Self::prompt_timeout_secs(),
        }
    }
}

/// Runtime tuning knobs read from `config.json` (`module_specific`). A small
/// snapshot keeps the values easy to thread into the functions that previously
/// held the hardcoded literals.
#[derive(Debug, Clone)]
struct Tuning {
    default_timeout_secs: i32,
    http_timeout_secs: u32,
    gateway_auth_backoff_base_secs: u32,
    gateway_auth_backoff_max_secs: u32,
    gateway_connect_retry_secs: u32,
    gateway_reconnect_delay_secs: u32,
    outbound_queue_cap: usize,
    send_worker_count: usize,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
    prompt_timeout_secs: u32,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            default_timeout_secs: DiscordAdapterConfig::default_timeout_secs(),
            http_timeout_secs: DiscordAdapterConfig::http_timeout_secs(),
            gateway_auth_backoff_base_secs: DiscordAdapterConfig::gateway_auth_backoff_base_secs(),
            gateway_auth_backoff_max_secs: DiscordAdapterConfig::gateway_auth_backoff_max_secs(),
            gateway_connect_retry_secs: DiscordAdapterConfig::gateway_connect_retry_secs(),
            gateway_reconnect_delay_secs: DiscordAdapterConfig::gateway_reconnect_delay_secs(),
            outbound_queue_cap: DiscordAdapterConfig::outbound_queue_cap(),
            send_worker_count: DiscordAdapterConfig::send_worker_count(),
            reconnect_base_secs: DiscordAdapterConfig::reconnect_base_secs(),
            reconnect_max_secs: DiscordAdapterConfig::reconnect_max_secs(),
            prompt_timeout_secs: DiscordAdapterConfig::prompt_timeout_secs(),
        }
    }
}

impl From<&DiscordAdapterConfig> for Tuning {
    fn from(cfg: &DiscordAdapterConfig) -> Self {
        Self {
            default_timeout_secs: cfg.default_timeout_secs,
            http_timeout_secs: cfg.http_timeout_secs,
            gateway_auth_backoff_base_secs: cfg.gateway_auth_backoff_base_secs,
            gateway_auth_backoff_max_secs: cfg.gateway_auth_backoff_max_secs,
            gateway_connect_retry_secs: cfg.gateway_connect_retry_secs,
            gateway_reconnect_delay_secs: cfg.gateway_reconnect_delay_secs,
            outbound_queue_cap: cfg.outbound_queue_cap,
            send_worker_count: cfg.send_worker_count,
            reconnect_base_secs: cfg.reconnect_base_secs,
            reconnect_max_secs: cfg.reconnect_max_secs,
            prompt_timeout_secs: cfg.prompt_timeout_secs,
        }
    }
}

/// Ensure every tuning key exists under `module_specific`, writing its default
/// when missing (mirrors the existing `embed_sends` backfill pattern).
fn backfill_tuning_defaults() {
    let path = PathBuf::from("config.json");
    let Ok(data) = std::fs::read_to_string(&path) else { return };
    let Ok(mut root) = serde_json::from_str::<serde_json::Value>(&data) else { return };
    let Some(obj) = root.as_object_mut() else { return };
    let ms = obj
        .entry("module_specific".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(ms) = ms.as_object_mut() else { return };
    let pairs: [(&str, serde_json::Value); 11] = [
        ("default_timeout_secs", serde_json::json!(DiscordAdapterConfig::default_timeout_secs())),
        ("http_timeout_secs", serde_json::json!(DiscordAdapterConfig::http_timeout_secs())),
        ("gateway_auth_backoff_base_secs", serde_json::json!(DiscordAdapterConfig::gateway_auth_backoff_base_secs())),
        ("gateway_auth_backoff_max_secs", serde_json::json!(DiscordAdapterConfig::gateway_auth_backoff_max_secs())),
        ("gateway_connect_retry_secs", serde_json::json!(DiscordAdapterConfig::gateway_connect_retry_secs())),
        ("gateway_reconnect_delay_secs", serde_json::json!(DiscordAdapterConfig::gateway_reconnect_delay_secs())),
        ("outbound_queue_cap", serde_json::json!(DiscordAdapterConfig::outbound_queue_cap())),
        ("send_worker_count", serde_json::json!(DiscordAdapterConfig::send_worker_count())),
        ("reconnect_base_secs", serde_json::json!(DiscordAdapterConfig::reconnect_base_secs())),
        ("reconnect_max_secs", serde_json::json!(DiscordAdapterConfig::reconnect_max_secs())),
        ("prompt_timeout_secs", serde_json::json!(DiscordAdapterConfig::prompt_timeout_secs())),
    ];
    let mut changed = false;
    for (key, default) in pairs {
        if !ms.contains_key(key) {
            ms.insert(key.to_string(), default);
            changed = true;
        }
    }
    if changed {
        let _ = std::fs::write(&path, serde_json::to_string_pretty(&root).unwrap_or_default());
    }
}

/// Expected `.env` keys for this adapter. Each is a SECRET with no meaningful
/// default, so it is written EMPTY as a placeholder — the operator (or the TUI
/// credential flow) fills the value in. A real environment variable always wins
/// over the file at load (load_env_file only sets unset vars).
const ENV_KEYS: &[&str] = &["DISCORD_BOT_TOKEN"];

/// Create `.env` if missing and ensure every expected key is present (empty
/// `KEY=` line). Existing keys/values are NEVER overwritten. The file is kept
/// owner-only (0o600 on unix) since it holds secrets.
fn ensure_env_file() {
    ensure_env_file_at(std::path::Path::new(".env"));
}

fn ensure_env_file_at(path: &std::path::Path) {
    let mut lines: Vec<String> = std::fs::read_to_string(path)
        .map(|c| c.lines().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    for key in ENV_KEYS {
        let prefix = format!("{}=", key);
        if !lines.iter().any(|l| l.trim().starts_with(&prefix)) {
            lines.push(prefix);
        }
    }
    let mut content = lines.join("\n");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    if std::fs::write(path, content).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

fn load_adapter_config() -> Option<DiscordAdapterConfig> {
    // The bot token is a secret → .env (loaded into env at startup, with a
    // config.json fallback for pre-migration installs). The servers map is
    // PUBLIC info → config.json.
    let saved = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .and_then(|v| v.get("module_specific").cloned())
        .unwrap_or_else(|| serde_json::json!({}));
    let legacy_token = saved.get("bot_token").and_then(|v| v.as_str()).unwrap_or("").to_string();

    let mut servers: std::collections::HashMap<String, Vec<String>> = saved
        .get("servers")
        .map(parse_servers_value)
        .unwrap_or_default();
    // Legacy single-server config: guild_id + channels → one servers entry.
    if servers.is_empty() {
        if let Some(guild) = saved.get("guild_id").and_then(|v| v.as_str()) {
            if !guild.is_empty() {
                let channels: Vec<String> = saved
                    .get("channels")
                    .and_then(|c| c.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|i| i.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                servers.insert(guild.to_string(), channels);
            }
        }
    }

    let bot_token = match std::env::var("DISCORD_BOT_TOKEN") {
        Ok(t) if !t.trim().is_empty() => t,
        _ => legacy_token,
    };

    // Public toggle: post SendToPlatforms as an embed. If the value isn't in
    // config.json yet, write the default (false) so the setting always exists.
    let embed_sends = saved.get("embed_sends").and_then(|v| v.as_bool()).unwrap_or_else(|| {
        if let Ok(data) = std::fs::read_to_string("config.json") {
            if let Ok(mut root) = serde_json::from_str::<serde_json::Value>(&data) {
                if let Some(obj) = root.as_object_mut() {
                    if let Some(ms) = obj
                        .entry("module_specific".to_string())
                        .or_insert_with(|| serde_json::json!({}))
                        .as_object_mut()
                    {
                        ms.entry("embed_sends".to_string()).or_insert_with(|| serde_json::json!(false));
                    }
                    let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
                }
            }
        }
        false
    });

    // Ensure the tuning knobs exist in config.json (created with their defaults
    // when missing), then read them (serde defaults for any still-absent key).
    backfill_tuning_defaults();
    let parsed: DiscordAdapterConfig = serde_json::from_value(saved.clone()).unwrap_or_default();

    Some(DiscordAdapterConfig {
        bot_token: Some(bot_token),
        servers,
        embed_sends,
        default_timeout_secs: parsed.default_timeout_secs,
        http_timeout_secs: parsed.http_timeout_secs,
        gateway_auth_backoff_base_secs: parsed.gateway_auth_backoff_base_secs,
        gateway_auth_backoff_max_secs: parsed.gateway_auth_backoff_max_secs,
        gateway_connect_retry_secs: parsed.gateway_connect_retry_secs,
        gateway_reconnect_delay_secs: parsed.gateway_reconnect_delay_secs,
        outbound_queue_cap: parsed.outbound_queue_cap,
        send_worker_count: parsed.send_worker_count,
        reconnect_base_secs: parsed.reconnect_base_secs,
        reconnect_max_secs: parsed.reconnect_max_secs,
        prompt_timeout_secs: parsed.prompt_timeout_secs,
    })
    .filter(|c| !c.bot_token.as_deref().unwrap_or("").is_empty())
}

fn save_adapter_config(
    bot_token: &str,
    servers: &std::collections::HashMap<String, ServerChannels>,
) {
    // The token is a secret → .env; the servers map is public → config.json.
    cockatiel_client::write_env_file(".env", &[("DISCORD_BOT_TOKEN", bot_token)]);
    let path = PathBuf::from("config.json");
    let mut json_val = if let Ok(data) = std::fs::read_to_string(&path) {
        serde_json::from_str::<serde_json::Value>(&data).unwrap_or_else(|_| json!({}))
    } else {
        json!({})
    };
    // Preserve any existing `module_specific` keys (tuning knobs, embed_sends)
    // so a save never drops them — only `servers` is (re)written.
    let mut spec = json_val
        .get("module_specific")
        .cloned()
        .and_then(|v| v.as_object().cloned())
        .map(serde_json::Value::Object)
        .unwrap_or_else(|| json!({}));
    spec["servers"] = serialize_servers(servers);
    json_val["module_specific"] = spec;
    if let Ok(pretty) = serde_json::to_string_pretty(&json_val) {
        let _ = std::fs::write(&path, pretty);
    }
}


/// Setup guide text — reused both as the printed guide (TUI log window, since
/// module stdout is captured) and as the `details` for engine prompts.
fn setup_guide_text() -> String {
    "\
==================================================
  Discord Adapter — Setup Required
==================================================
  1. Create a bot at https://discord.com/developers/applications
     - New Application -> Bot -> Reset Token, copy it.
  2. Enable the \"Message Content\" intent (Privileged Gateway Intents).
  3. Invite the bot to your server:
     OAuth2 -> URL Generator -> scopes: bot (+ applications.commands)
     Permissions: Read Messages, Send Messages, Moderate Members.
  4. Enable Developer Mode: Discord Settings -> Advanced -> Developer Mode.
     - Right-click your server -> Copy Server ID.
     - (Optional) Right-click a channel -> Copy Channel ID.
  5. In the TUI: select discord-adapter -> press c, then enter:
     - Bot Token (discord.com/developers)
     - Server (Guild) ID
     - Channel IDs (one per line; empty = monitor all channels)
=================================================="
        .to_string()
}

/// Clean a command target: `<@!123>` / `<@123>` mentions -> id, `@name` -> name.
fn clean_target(raw: &str) -> String {
    let mut t = raw.trim().to_string();
    if t.starts_with("<@") {
        t = t.trim_start_matches("<@").trim_start_matches('!').to_string();
        t = t.trim_end_matches('>').to_string();
    } else {
        t = t.trim_start_matches('@').to_string();
    }
    t
}

/// Build a moderator query from the engine-routed command. The engine already
/// parsed + routed `!ban`/`!timeout`; here we map command_name -> query and
/// extract the target/reason from the message args (no re-parsing). The actor
/// (message author) is included.
fn build_mod_query(
    command_name: &str,
    message: &str,
    author: &str,
    default_timeout_secs: i32,
) -> Option<(String, serde_json::Value)> {
    let mut tokens = message.split_whitespace();
    let _cmd = tokens.next()?;
    match command_name {
        "ban" => {
            let target = clean_target(tokens.next()?);
            if target.is_empty() {
                return None;
            }
            // Discord bans are permanent — a timed ban (`!ban @user -d 300`) is
            // a timeout instead (matches how the engine's mod_timeout works).
            let mut duration_secs: Option<i32> = None;
            let mut reason = tokens.collect::<Vec<_>>().join(" ");
            if let Some(dpos) = reason.find("-d") {
                let after = reason[dpos + 2..].trim();
                let (num, _) = after.split_once(char::is_whitespace).unwrap_or((after, ""));
                if let Ok(secs) = num.parse::<i32>() {
                    duration_secs = Some(secs.max(1));
                    reason = format!("{}{}", reason[..dpos].trim(), after[num.len()..].trim());
                }
            }
            if let Some(duration_secs) = duration_secs {
                return Some((
                    "mod_timeout".to_string(),
                    serde_json::json!({
                        "platform": "discord",
                        "handle": target,
                        "duration_secs": duration_secs,
                        "reason": reason,
                        "actor": { "platform": "discord", "handle": author },
                    }),
                ));
            }
            Some((
                "mod_ban".to_string(),
                serde_json::json!({
                    "platform": "discord",
                    "handle": target,
                    "reason": reason,
                    "actor": { "platform": "discord", "handle": author },
                }),
            ))
        }

        "timeout" => {
            let target = clean_target(tokens.next()?);
            if target.is_empty() {
                return None;
            }
            let mut duration_secs = default_timeout_secs;
            let mut reason = String::new();
            if let Some(d) = tokens.next() {
                if let Ok(secs) = d.parse::<i32>() {
                    duration_secs = secs;
                } else {
                    reason = d.to_string();
                }
            }
            let rest: Vec<&str> = tokens.collect();
            if !rest.is_empty() {
                if !reason.is_empty() {
                    reason = format!("{} {}", reason, rest.join(" "));
                } else {
                    reason = rest.join(" ");
                }
            }
            Some((
                "mod_timeout".to_string(),
                serde_json::json!({
                    "platform": "discord",
                    "handle": target,
                    "duration_secs": duration_secs,
                    "reason": reason,
                    "actor": { "platform": "discord", "handle": author },
                }),
            ))
        }

        _ => None,
    }
}

// ── Discord REST helpers ──────────────────────────────────────────────

/// The `Authorization` header value Discord expects for a BOT token. The
/// scheme is `Bot` — `Bearer` is the USER-token scheme, and Discord answers a
/// bot token sent as `Bearer` with 401 Unauthorized.
fn bot_auth_value(token: &str) -> String {
    format!("Bot {}", token)
}

/// Attach Discord's bot `Authorization` header to an outgoing request. reqwest
/// has no typed helper for the `Bot` scheme, so every REST call in this module
/// routes through here (all of which would otherwise use `bearer_auth`).
fn bot_auth(req: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
    req.header(reqwest::header::AUTHORIZATION, bot_auth_value(token))
}

/// Push a guild's approximate member count to the engine via a `ChannelStats`
/// payload. The engine stores it and serves it to other modules through the
/// `channel_viewers` virtual query. Reads the session identity at send time.
async fn push_channel_stats(
    write_ws: &Arc<Mutex<WsWriteHalf>>,
    identity: &Arc<Mutex<EngineIdentity>>,
    platform: &str,
    channel: &str,
    viewers: i64,
    is_live: bool,
    title: &str,
) {
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth,
        module_name: module,
        module_instance_uuid7: instance,
        payload: Some(EnginePayload::ChannelStats(cockatiel_client::proto::ChannelStats {
            platform: platform.to_string(),
            channel: channel.to_string(),
            viewers,
            is_live,
            title: title.to_string(),
            updated_at: now_unix_millis(),
        })),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_ws.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Build a client for Discord REST calls: a hard per-request timeout so a hung
/// connection can't wedge a task forever, plus the User-Agent Discord
/// documents as mandatory. A builder failure falls back to the default client
/// rather than taking the module down.
fn build_http_client(http_timeout_secs: u32) -> reqwest::Client {
    match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs as u64))
        .user_agent(USER_AGENT)
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            error!(
                "Could not build the Discord HTTP client ({}). Falling back to the default client.",
                e
            );
            reqwest::Client::new()
        }
    }
}

async fn send_discord_message(
    client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    msg: &str,
    embed: bool,
    timeout_secs: u32,
) -> Result<(), String> {
    // `embed_sends: true` posts a discordjs-style embed instead of a plain
    // message (ROADMAP: "receive a Send and post as an embed").
    let body = if embed {
        json!({ "embeds": [{ "description": msg }] })
    } else {
        json!({ "content": msg })
    };
    let resp = bot_auth(
        client.post(format!("{}/channels/{}/messages", REST_API, channel_id)),
        token,
    )
    .json(&body)
    .timeout(std::time::Duration::from_secs(timeout_secs as u64))
    .send()
    .await
    .map_err(|e| format!("send request failed: {}", e))?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let text = resp.text().await.unwrap_or_default();
        Err(format!("Discord send {}: {}", status, text))
    }
}

// ── Discord REST helpers (setup-time validation) ──────────────────────

/// Why a bot-token check failed. The variants are deliberately distinct: a
/// rejected token (clear it and re-prompt) must never be conflated with a
/// transient condition (keep it and retry), or a network blip / rate limit
/// silently costs the operator a perfectly good token.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthFailure {
    /// HTTP 401/403 — the token itself is not usable.
    Unauthorized { status: u16 },
    /// HTTP 429 — Discord asked us to slow down; the token is untouched.
    RateLimited { retry_after_secs: u64 },
    /// Any other unexpected status (5xx and friends) — the token is untouched.
    Server { status: u16 },
    /// DNS/TLS/connect/timeout — we never got an answer from Discord.
    Transport { detail: String },
}

/// The delay a 429 asked for, in whole seconds rounded UP (never below 1s).
/// Discord sends it as JSON in the BODY for bot tokens
/// (`{"message":..,"retry_after":1.234,"global":false}`); an absent or
/// unparseable body falls back to a conservative 5s. Never panics.
fn retry_after_secs_from_body(body: Option<&str>) -> u64 {
    const FALLBACK: u64 = 5;
    let Some(body) = body else { return FALLBACK };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return FALLBACK;
    };
    let Some(secs) = v.get("retry_after").and_then(|r| r.as_f64()) else {
        return FALLBACK;
    };
    secs.ceil().max(1.0) as u64
}

/// A human-readable cause for the operator, naming the real reason instead of
/// a bare "Discord rejected the token".
fn describe_auth_failure(failure: &AuthFailure) -> String {
    match failure {
        AuthFailure::Unauthorized { status } => format!(
            "HTTP {} — token revoked, or it belongs to a different bot application",
            status
        ),
        AuthFailure::RateLimited { retry_after_secs } => format!(
            "HTTP 429 — Discord rate limited us, retrying in {}s",
            retry_after_secs
        ),
        AuthFailure::Server { status } => {
            format!("HTTP {} — Discord API error, the token itself looks fine", status)
        }
        AuthFailure::Transport { detail } => {
            format!("could not reach discord.com: {}", detail)
        }
    }
}

/// Verify a bot token with `GET {base_url}/users/@me`, reporting WHY it failed.
/// `base_url` is [`REST_API`] in production; tests point it at a mock server.
async fn check_discord_auth(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
) -> Result<(), AuthFailure> {
    let resp = bot_auth(client.get(format!("{}/users/@me", base_url)), token)
        .timeout(std::time::Duration::from_secs(AUTH_CHECK_TIMEOUT_SECS as u64))
        .send()
        .await
        .map_err(|e| AuthFailure::Transport { detail: e.to_string() })?;

    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let code = status.as_u16();
    match code {
        401 | 403 => Err(AuthFailure::Unauthorized { status: code }),
        429 => {
            let body = resp.text().await.ok();
            Err(AuthFailure::RateLimited {
                retry_after_secs: retry_after_secs_from_body(body.as_deref()),
            })
        }
        _ => Err(AuthFailure::Server { status: code }),
    }
}

/// The server (guild) IDs the bot currently belongs to.
async fn list_bot_guilds(client: &reqwest::Client, token: &str) -> Vec<String> {
    let Ok(resp) = bot_auth(
        client.get(format!("{}/users/@me/guilds", REST_API)),
        token,
    )
    .send()
    .await
    else {
        return Vec::new();
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    v.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|g| g.get("id").and_then(|i| i.as_str()))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The channel IDs the bot can see in a server (guild).
async fn guild_channels(client: &reqwest::Client, token: &str, guild_id: &str) -> Vec<String> {
    let Ok(resp) = bot_auth(
        client.get(format!("{}/guilds/{}/channels", REST_API, guild_id)),
        token,
    )
    .send()
    .await
    else {
        return Vec::new();
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    v.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.get("id").and_then(|i| i.as_str()))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Parse the member count out of a `GET /guilds/{id}?with_counts=true` body.
fn guild_member_count_from_body(v: &serde_json::Value) -> Option<i64> {
    v.get("approximate_member_count").and_then(|c| c.as_i64())
}

/// Fetch a guild's approximate member count via `GET /guilds/{id}?with_counts=true`.
/// This is the "members who have access to the server" metric the adapter
/// reports in place of a viewer count (Discord has no live-stream concept).
async fn guild_member_count(
    client: &reqwest::Client,
    token: &str,
    guild_id: &str,
) -> Option<i64> {
    let Ok(resp) = bot_auth(
        client.get(format!("{}/guilds/{}?with_counts=true", REST_API, guild_id)),
        token,
    )
    .send()
    .await
    else {
        return None;
    };
    if !resp.status().is_success() {
        return None;
    }
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return None;
    };
    guild_member_count_from_body(&v)
}

/// Text-capable channel IDs in a server (GUILD_TEXT + GUILD_ANNOUNCEMENT),
/// used to resolve a `ServerChannels::All` ("*") policy into concrete send
/// targets. Voice/category/forum channels can't receive plain messages.
async fn guild_text_channels(client: &reqwest::Client, token: &str, guild_id: &str) -> Vec<String> {
    let Ok(resp) = bot_auth(
        client.get(format!("{}/guilds/{}/channels", REST_API, guild_id)),
        token,
    )
    .send()
    .await
    else {
        return Vec::new();
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    v.as_array()
        .map(|arr| {
            arr.iter()
                .filter(|c| {
                    let ty = c.get("type").and_then(|t| t.as_u64()).unwrap_or(1);
                    ty == 0 || ty == 5
                })
                .filter_map(|c| c.get("id").and_then(|i| i.as_str()))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The operator's choice when a configured server/channel is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TieChoice {
    /// Re-test the same id (the failure may be transient).
    Retry,
    /// Keep the entry but skip it for now (noted in the log).
    Ignore,
    /// Remove the entry from the config.
    Remove,
    /// Prompt for a replacement value, save it, and re-test.
    Edit,
}

fn parse_tie_choice(answer: &str) -> Option<TieChoice> {
    match answer.trim().to_ascii_lowercase().as_str() {
        "t" | "try" | "retry" | "try again" => Some(TieChoice::Retry),
        "i" | "ignore" => Some(TieChoice::Ignore),
        "r" | "remove" => Some(TieChoice::Remove),
        "e" | "edit" => Some(TieChoice::Edit),
        _ => None,
    }
}

fn tie_choices_help() -> String {
    "Enter one of: (t)ry again, (i)gnore, (r)emove, (e)dit".to_string()
}

// ── Discord gateway ───────────────────────────────────────────────────

/// Connect to the Discord gateway, identify, heartbeat, and forward
/// MESSAGE_CREATE events to the engine as preprocessed messages. Receive-only:
/// the token + servers/channels were resolved + validated in the linear setup
/// phase, so this loop never prompts (no reconnect re-prompt spin).
async fn run_discord_gateway(
    client: reqwest::Client,
    token: String,
    servers: std::collections::HashMap<String, ServerChannels>,
    engine_write: Arc<Mutex<WsWriteHalf>>,
    send_channels: Arc<Mutex<Vec<String>>>,
    identity: Arc<Mutex<EngineIdentity>>,
    tuning: &Tuning,
) {
    // Exponential backoff across repeated auth rejections (4004/4005/op 9):
    // a fixed retry with the same bad token hammers the gateway and can
    // trip Discord's 4008 rate limit. Normal drops keep the fixed delay.
    let mut auth_backoff = tuning.gateway_auth_backoff_base_secs;
    loop {
        let mut auth_failed = false;
        let (mut ws, _) = match tokio_tungstenite::connect_async(GATEWAY_URL).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Failed to connect to Discord gateway ({}). Likely causes: invalid bot token, the Message Content intent is not enabled in the Developer Portal, or the bot is not in the guild. Retrying in {}s...",
                    e, tuning.gateway_connect_retry_secs
                );
                tokio::time::sleep(std::time::Duration::from_secs(
                    tuning.gateway_connect_retry_secs as u64,
                ))
                .await;
                continue;
            }
        };

        // Wait for OP 10 HELLO for the heartbeat interval, then IDENTIFY.
        let mut heartbeat_interval_ms = 41250u64;
        let mut seq: Option<u64> = None;

        // First read the HELLO.
        match ws.next().await {
            Some(Ok(WsMessage::Text(txt))) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                    if v.get("op").and_then(|o| o.as_u64()) == Some(10) {
                        heartbeat_interval_ms = v["d"]["heartbeat_interval"].as_u64().unwrap_or(41250);
                    }
                }
            }
            Some(Ok(WsMessage::Binary(_))) => {}
            _ => {}
        }

        let identify = json!({
            "op": 2,
            "d": {
                "token": &token,
                "intents": DISCORD_INTENTS,
                "properties": { "os": "linux", "browser": "cockatiel", "device": "cockatiel" },
            }
        });
        if ws.send(WsMessage::Text(identify.to_string())).await.is_err() {
            continue;
        }
        info!("Discord gateway identified. Heartbeat every {} ms.", heartbeat_interval_ms);

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval_ms));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        'conn: loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    let _ = ws.send(WsMessage::Text(json!({ "op": 1, "d": seq }).to_string())).await;
                }
                msg = ws.next() => {
                    let Some(msg) = msg else { break 'conn; };
                    let Ok(msg) = msg else { break 'conn; };
                    let text = match msg {
                        WsMessage::Text(t) => t,
                        WsMessage::Binary(b) => String::from_utf8_lossy(&b).to_string(),
                        WsMessage::Close(frame) => {
                            if let Some(f) = &frame {
                                warn!(
                                    "Discord gateway closed the connection: code={} reason={}",
                                    f.code, f.reason
                                );
                                let code: u16 = f.code.into();
                                if code == 4004 || code == 4005 {
                                    error!("Discord gateway rejected the bot token (close code {}). The token is invalid or the Message Content intent is missing — fix it in the Discord Developer Portal / module .env.", code);
                                    auth_failed = true;
                                }
                            } else {
                                warn!("Discord gateway closed the connection (no close frame)");
                            }
                            break 'conn;
                        }
                        _ => continue,
                    };
                    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&text) else {
                        continue;
                    };
                    let op = payload.get("op").and_then(|o| o.as_u64()).unwrap_or(0);
                    if let Some(s) = payload.get("s").and_then(|s| s.as_u64()) {
                        seq = Some(s);
                    }
                    // op 9 = Invalid Session (bad token / session expired). A
                    // rejected token makes this loop forever if we reconnect on
                    // a fixed timer, so treat it like an auth failure (backoff).
                    if op == 9 {
                        error!("Discord gateway reported an invalid session (op 9) — the bot token is likely wrong.");
                        auth_failed = true;
                        break 'conn;
                    }
                    if op == 0 {
                        let t = payload.get("t").and_then(|v| v.as_str()).unwrap_or("");
                        let d = payload.get("d");
                        match t {
                            "READY" => {
                                info!("Discord gateway ready (bot: {}).", d.and_then(|x| x.get("user")).and_then(|u| u.get("username")).and_then(|u| u.as_str()).unwrap_or("?"));
                                // Server/channel setup + validation happened in the
                                // linear setup phase — the gateway is receive-only.
                                let desc: Vec<String> = servers
                                    .iter()
                                    .map(|(g, p)| match p {
                                        ServerChannels::All => format!("{}=[*]", g),
                                        ServerChannels::Some(chs) => format!("{}={}", g, chs.join(",")),
                                    })
                                    .collect();
                                // Resolve the full send-target list, expanding
                                // `*` servers to their text channels so engine
                                // replies are never silently dropped.
                                *send_channels.lock().await = resolve_send_channels(&client, &token, &servers).await;
                                info!("Monitoring servers: {}", desc.join(" · "));
                            }
                            "MESSAGE_CREATE" => {
                                if let Some(d) = d {
                                    handle_message_create(d, &servers, &engine_write, &identity).await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        if auth_failed {
            error!(
                "Discord gateway auth failed — the bot token is rejected. Backing off {}s before retrying (a fixed retry would hammer the gateway / trip close code 4008).",
                auth_backoff
            );
            tokio::time::sleep(std::time::Duration::from_secs(auth_backoff as u64)).await;
            auth_backoff = (auth_backoff * 2).min(tuning.gateway_auth_backoff_max_secs);
        } else {
            auth_backoff = tuning.gateway_auth_backoff_base_secs;
            warn!(
                "Discord gateway disconnected. Reconnecting in {}s...",
                tuning.gateway_reconnect_delay_secs
            );
            tokio::time::sleep(std::time::Duration::from_secs(
                tuning.gateway_reconnect_delay_secs as u64,
            ))
            .await;
        }
    }
}

/// Collect image URLs from a Discord MESSAGE_CREATE payload: file attachments
/// plus embed image/thumbnail URLs, filtered to the image types the display
/// modules render (matching term-chat's extractor).
fn extract_image_urls(d: &serde_json::Value) -> Vec<String> {
    fn is_image_url(u: &str) -> bool {
        let base = u.split('?').next().unwrap_or(u).to_ascii_lowercase();
        [".png", ".jpg", ".jpeg", ".gif", ".webp"].iter().any(|ext| base.ends_with(ext))
    }
    let mut urls: Vec<String> = Vec::new();
    if let Some(atts) = d.get("attachments").and_then(|a| a.as_array()) {
        for a in atts {
            if let Some(u) = a.get("url").and_then(|u| u.as_str()) {
                if is_image_url(u) {
                    urls.push(u.to_string());
                }
            }
        }
    }
    if let Some(embeds) = d.get("embeds").and_then(|e| e.as_array()) {
        for e in embeds {
            for key in ["image", "thumbnail"] {
                if let Some(u) = e
                    .get(key)
                    .and_then(|i| i.get("url"))
                    .and_then(|u| u.as_str())
                {
                    if is_image_url(u) {
                        urls.push(u.to_string());
                    }
                }
            }
        }
    }
    urls
}

async fn handle_message_create(
    d: &serde_json::Value,
    servers: &std::collections::HashMap<String, ServerChannels>,
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    identity: &Arc<Mutex<EngineIdentity>>,
) {
    let is_bot = d.get("author").and_then(|a| a.get("bot")).and_then(|b| b.as_bool()).unwrap_or(false);
    if is_bot {
        return;
    }
    // Forward only messages from CONFIGURED servers, honoring each server's
    // channel policy (`*`/bare = every channel in that server).
    let msg_guild = d.get("guild_id").and_then(|g| g.as_str()).unwrap_or("");
    let channel_id = d.get("channel_id").and_then(|c| c.as_str()).unwrap_or("");
    let allowed = match servers.get(msg_guild) {
        None => false,
        Some(ServerChannels::All) => true,
        Some(ServerChannels::Some(chs)) => chs.iter().any(|c| c == channel_id),
    };
    if !allowed {
        return;
    }
    let author = d.get("author").and_then(|a| a.get("username")).and_then(|u| u.as_str()).unwrap_or("Unknown");
    let content = d.get("content").and_then(|c| c.as_str()).unwrap_or("").trim().to_string();

    // Uploaded images arrive as attachments / embeds, not in `content`. Collect
    // their URLs and append them to the message text so display modules
    // (term-chat) can convert them to images, exactly like an image link.
    let image_urls = extract_image_urls(d);
    // A message that is ONLY an image still has nothing to show if the image
    // couldn't be resolved — drop it then.
    if content.is_empty() && image_urls.is_empty() {
        return;
    }
    let mut raw_message = content.clone();
    for url in &image_urls {
        if !raw_message.is_empty() {
            raw_message.push(' ');
        }
        raw_message.push_str(url);
    }

    let pre = MessagePreProcess {
        audio: vec![],
        audio_type: String::new(),
        message_uuid7: String::new(),
        raw_message: Some(ChatMessage {
            platform: "discord".into(),
            raw_data: serde_json::to_string(d).unwrap_or_default().into_bytes(),
            raw_message: raw_message.clone(),
            user_uuid7: author.to_string(),
            command: None,
            channel_id: channel_id.to_string(),
            user_data: None,
        }),
    };
    // Use the CURRENT session identity (a reconnect swaps it) so platform
    // sends never carry a stale token after an engine reconnection.
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth,
        module_name: module,
        module_instance_uuid7: instance,
        payload: Some(EnginePayload::MessagePreProcess(pre)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut write = engine_write.lock().await;
        let _ = write.send(WsMessage::Binary(buf)).await;
    }
}

// ── main ──────────────────────────────────────────────────────────────

/// Send a Prompt to the engine (forwarded to connected UIs) and wait for the
/// operator's response (`PromptResponse.reason`). Returns None on cancel/timeout.
#[allow(clippy::too_many_arguments)]
async fn prompt_for_input(
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    title: &str,
    details: &str,
    input_label: &str,
    kind: PromptKind,
    timeout: u32,
) -> Option<String> {
    let prompt_id = uuid::Uuid::now_v7().to_string();
    let prompt_type = match kind {
        PromptKind::Boolean => PromptType::Boolean,
        PromptKind::String => PromptType::String,
        PromptKind::Credential => PromptType::Credential,
    };
    let prompt = Prompt {
        prompt_id_uuid7: prompt_id.clone(),
        prompt: title.to_string(),
        details: details.to_string(),
        yes_dialog: "Submit".to_string(),
        no_dialog: "Cancel".to_string(),
        timeout,
        origin: module_name.to_string(),
        origin_uuid7: String::new(),
        instructions: String::new(),
        link: String::new(),
        input_label: input_label.to_string(),
        prompt_type: prompt_type as i32,
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: instance_uuid.to_string(),
        payload: Some(EnginePayload::Prompt(prompt)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_err() {
        return None;
    }
    {
        let mut write = engine_write.lock().await;
        if write.send(WsMessage::Binary(buf)).await.is_err() {
            return None;
        }
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout as u64 + 10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(10), prompt_rx.recv()).await {
            Ok(Some(resp)) if resp.prompt_id_uuid7 == prompt_id => {
                return if resp.accepted {
                    Some(resp.reason)
                } else {
                    None
                };
            }
            Ok(Some(_)) => continue, // a different prompt's response
            Ok(None) => return None,
            // The 10s poll interval elapsed with no response yet: keep waiting
            // until the real deadline rather than auto-cancelling.
            Err(_) => continue,
        }
    }
    None
}

// ── Linear setup phase ────────────────────────────────────────────────
// Resolve the bot token (prompt + verify) and the server→channel map
// (validate every entry with the t/i/r/e choices) BEFORE the receive loop,
// so the gateway never re-prompts in a reconnect loop.

/// What a failed token check means for the setup loop. `ClearToken` is the
/// ONLY outcome that discards a token or advances the rejection budget —
/// a rate limit, a 5xx, or an unreachable network must never be reported as
/// (or cost) a wrong token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthAction {
    /// The token is genuinely rejected: drop it and ask the operator for a new
    /// one.
    ClearToken,
    /// Keep the current token and retry it after the given delay.
    RetryAfter(u64),
    /// Consecutive rejections exhausted the attempt budget: give up so the
    /// caller exits and the supervisor can restart the module.
    GiveUp,
}

/// Transient backoff for a `Server`/`Transport` failure: 2s, doubling, capped
/// at 30s. Pure so the policy is testable without a network or a clock.
fn transient_backoff_secs(attempt: u32) -> u64 {
    (2u64.saturating_mul(1u64 << attempt.min(4))).min(30)
}

/// Decide what to do about a failed token check. `rejections` is the number of
/// consecutive `Unauthorized` results so far, `transient` the number of
/// consecutive rate-limit/server/transport results (which only drives the
/// backoff curve). A transient failure leaves `rejections` untouched, so a
/// correct token is never punished for the network being down.
fn auth_retry_action(
    failure: &AuthFailure,
    rejections: u32,
    transient: u32,
) -> AuthAction {
    match failure {
        AuthFailure::Unauthorized { .. } if rejections + 1 >= MAX_AUTH_ATTEMPTS => {
            AuthAction::GiveUp
        }
        AuthFailure::Unauthorized { .. } => AuthAction::ClearToken,
        AuthFailure::RateLimited { retry_after_secs } => {
            AuthAction::RetryAfter((*retry_after_secs).max(1))
        }
        AuthFailure::Server { .. } | AuthFailure::Transport { .. } => {
            AuthAction::RetryAfter(transient_backoff_secs(transient))
        }
    }
}

/// Whether a freshly submitted token should get a clean rejection budget.
///
/// A token that DIFFERS from the one Discord just rejected is a new attempt and
/// deserves the full [`MAX_AUTH_ATTEMPTS`]. Re-submitting the very same
/// rejected token does not, so the counter keeps climbing and the setup ends
/// after [`MAX_AUTH_ATTEMPTS`] identical rejections. Pure so the policy is
/// testable without a network or a prompt.
fn should_reset_rejection_budget(last_rejected: Option<&str>, submitted: &str) -> bool {
    last_rejected != Some(submitted)
}

/// Why the setup phase could not produce a validated token. Every variant is
/// fatal: the caller logs it and exits so the supervisor restarts the module.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthSetupError {
    /// The operator submitted an empty token.
    EmptyToken,
    /// The operator cancelled the prompt, or it timed out.
    Cancelled,
    /// The same token was rejected [`MAX_AUTH_ATTEMPTS`] times in a row —
    /// carries the real cause of the last one.
    Rejected(String),
    /// The operator ran out of attempts: [`MAX_AUTH_SUBMISSIONS`] tokens
    /// submitted, none accepted.
    TooManySubmissions { submitted: u32 },
}

impl std::fmt::Display for AuthSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthSetupError::EmptyToken => write!(
                f,
                "Discord bot token setup cancelled (no token provided). Exiting so the supervisor can restart the module."
            ),
            AuthSetupError::Cancelled => write!(
                f,
                "Discord bot token setup cancelled by the operator. Exiting so the supervisor can restart the module."
            ),
            AuthSetupError::Rejected(cause) => write!(
                f,
                "Discord bot token setup failed after {} rejections of the same token (last: {}). Exiting so the supervisor can restart the module.",
                MAX_AUTH_ATTEMPTS, cause
            ),
            AuthSetupError::TooManySubmissions { submitted } => write!(
                f,
                "Discord bot token setup gave up after {} submitted tokens, none accepted. Exiting so the supervisor can restart the module.",
                submitted
            ),
        }
    }
}

/// Expand a `*` guild key into the concrete guild ids the bot is actually in,
/// returning a log line (or `None` if there was no `*` key to expand).
///
/// This expansion is mandatory, not cosmetic. Every consumer downstream matches
/// on the real guild id from a MESSAGE_CREATE payload:
///   - the receive filter is a literal `servers.get(guild_id)` lookup, so a `*`
///     key can never match and every message is dropped;
///   - the `resolve_servers` validation loop tests `bot_guilds.contains(guild)`,
///     which `*` always fails, producing a bogus "not in the bot's servers"
///     prompt;
///   - the send resolver calls GET /guilds/*/channels, which Discord rejects
///     with 400 "guild_id must be a NUMBER".
fn expand_guild_wildcard(
    servers: &mut std::collections::HashMap<String, ServerChannels>,
    bot_guilds: &[String],
) -> Option<String> {
    let policy = servers.remove("*")?;
    // `*=ch1,ch2` is meaningless — channel ids are per-server, so one list
    // applied to every guild would match nothing. Fall back to "all channels"
    // and say so, rather than silently monitoring nothing.
    let policy = match policy {
        ServerChannels::Some(_) => {
            let policy = ServerChannels::All;
            for guild in bot_guilds {
                servers.entry(guild.clone()).or_insert_with(|| policy.clone());
            }
            return Some(format!(
                "server wildcard '*' had an explicit channel list, which cannot apply across \
                 servers — monitoring every channel of all {} server(s) instead\n",
                bot_guilds.len()
            ));
        }
        other => other,
    };
    if bot_guilds.is_empty() {
        return Some(
            "server wildcard '*' expanded to 0 servers — the bot is not in any server\n".to_string(),
        );
    }
    for guild in bot_guilds {
        servers.entry(guild.clone()).or_insert_with(|| policy.clone());
    }
    Some(format!(
        "server wildcard '*' expanded to {} server(s): {}\n",
        bot_guilds.len(),
        bot_guilds.join(", ")
    ))
}

/// What the setup phase should do with the configured servers, decided with NO
/// I/O so it is directly testable. `resolve_servers` turns a plan into prompts.
///
/// This is the decision layer that used to be inseparable from the websocket
/// work: the `*` wildcard expansion, the "nothing configured" branch, and the
/// split between guilds the bot is in and guilds it isn't. All three were
/// unreachable from a unit test, which is exactly how `{"*": ["*"]}` shipped
/// silently monitoring zero servers.
#[derive(Debug, PartialEq, Eq)]
enum ServerPlan {
    /// Nothing is configured and the bot is in no servers at all.
    NothingToMonitor,
    /// Nothing is configured, but the bot is in at least one server — ask the
    /// operator to pick one.
    Pick,
    /// Guild-level classification. `valid` are guilds the bot is in; `invalid`
    /// are configured guilds it isn't, and each of those still needs an
    /// operator decision. Every entry in `valid` still needs its CHANNELS
    /// validated, which is the socket-bound half of the job.
    Resolved {
        valid: std::collections::HashMap<String, ServerChannels>,
        invalid: Vec<(String, ServerChannels)>,
    },
}

/// Expand the `*` wildcard in `servers`, then split what remains into guilds
/// the bot is in and guilds it is not. `servers` is mutated in place so the
/// caller keeps the expanded map. Returns the plan plus any operator-facing
/// notes (e.g. how far the wildcard expanded) to append to the setup log.
fn plan_server_monitoring(
    servers: &mut std::collections::HashMap<String, ServerChannels>,
    bot_guilds: &[String],
) -> (ServerPlan, Vec<String>) {
    let mut notes = Vec::new();
    if let Some(line) = expand_guild_wildcard(servers, bot_guilds) {
        notes.push(line);
    }
    if servers.is_empty() {
        let plan = if bot_guilds.is_empty() {
            ServerPlan::NothingToMonitor
        } else {
            ServerPlan::Pick
        };
        return (plan, notes);
    }
    let mut valid: std::collections::HashMap<String, ServerChannels> = Default::default();
    let mut invalid: Vec<(String, ServerChannels)> = Vec::new();
    for (guild, policy) in servers.iter() {
        if bot_guilds.contains(guild) {
            valid.insert(guild.clone(), policy.clone());
        } else {
            invalid.push((guild.clone(), policy.clone()));
        }
    }
    // Stable order so the operator sees the same prompts every run.
    invalid.sort_by(|a, b| a.0.cmp(&b.0));
    (ServerPlan::Resolved { valid, invalid }, notes)
}

/// Prompt for (and verify) a Discord bot token, then RETURN the validated one.
/// The caller persists it — this function never writes `.env`, so a token is
/// only ever saved after Discord has accepted it. `base_url` is [`REST_API`] in
/// production; tests point it at a mock server.
#[allow(clippy::too_many_arguments)]
async fn resolve_auth(
    client: &reqwest::Client,
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    base_url: &str,
    mut bot_token: String,
) -> Result<String, AuthSetupError> {
    // Three independent counters, deliberately not conflated:
    //  - `rejections`: consecutive Unauthorized results for the CURRENT token.
    //    Only a genuinely different submission resets it.
    //  - `transient`: consecutive rate-limit/server/transport results. It is
    //    NEVER reset by a new token, so a long outage keeps the backoff
    //    climbing instead of re-probing Discord at the base delay forever.
    //  - `submissions`: tokens the operator has submitted this run, the hard
    //    ceiling that guarantees the prompt loop terminates.
    let mut rejections = 0u32;
    let mut transient = 0u32;
    let mut submissions = 0u32;
    let mut last_rejected: Option<String> = None;
    loop {
        if !bot_token.is_empty() {
            match check_discord_auth(client, base_url, &bot_token).await {
                Ok(()) => return Ok(bot_token),
                Err(failure) => {
                    let cause = describe_auth_failure(&failure);
                    match auth_retry_action(&failure, rejections, transient) {
                        AuthAction::ClearToken => {
                            rejections += 1;
                            error!(
                                "Discord rejected the bot token: {} — prompting for a fresh one (attempt {} of {} for this token).",
                                cause, rejections, MAX_AUTH_ATTEMPTS
                            );
                            last_rejected = Some(std::mem::take(&mut bot_token));
                        }
                        AuthAction::RetryAfter(secs) => {
                            transient += 1;
                            warn!(
                                "Could not verify the bot token: {} — keeping the current token and retrying in {}s.",
                                cause, secs
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                            continue;
                        }
                        AuthAction::GiveUp => {
                            return Err(AuthSetupError::Rejected(cause));
                        }
                    }
                }
            }
        }
        if submissions >= MAX_AUTH_SUBMISSIONS {
            return Err(AuthSetupError::TooManySubmissions { submitted: submissions });
        }
        match prompt_for_input(
            engine_write,
            prompt_rx,
            auth_token,
            module_name,
            instance_uuid,
            "Discord Bot Token Required",
            &setup_guide_text(),
            "Bot Token",
            PromptKind::Credential,
            120,
        )
        .await
        {
            Some(val) => {
                let t = val.trim().to_string();
                if t.is_empty() {
                    // Empty input is a cancel — the caller exits so the
                    // supervisor can restart the module instead of pinning
                    // setup forever.
                    return Err(AuthSetupError::EmptyToken);
                }
                if should_reset_rejection_budget(last_rejected.as_deref(), &t) {
                    rejections = 0;
                }
                last_rejected = Some(t.clone());
                submissions += 1;
                bot_token = t;
            }
            // Operator cancelled / prompt timed out — same clean exit.
            None => return Err(AuthSetupError::Cancelled),
        }
    }
}

/// Ask the operator how to handle an invalid entry: (t)ry / (i)gnore /
/// (r)emove / (e)dit. Reprompts until a valid choice (or None on cancel).
async fn prompt_tie_choice(
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    subject: &str,
    prompt_timeout_secs: u32,
) -> Option<TieChoice> {
    loop {
        let answer = prompt_for_input(
            engine_write,
            prompt_rx,
            auth_token,
            module_name,
            instance_uuid,
            "Invalid Discord Entry",
            &format!("{}\n\n{}", subject, tie_choices_help()),
            "t / i / r / e",
            PromptKind::String,
            prompt_timeout_secs,
        )
        .await;
        match answer.as_deref().and_then(parse_tie_choice) {
            Some(c) => return Some(c),
            None if answer.is_some() => {
                warn!("Unrecognized choice — expected t / i / r / e.");
            }
            None => return None, // cancelled
        }
    }
}

/// No servers configured (or all were invalid): pick one from the bot's guilds
/// + choose its channels (empty = all).
#[allow(clippy::too_many_arguments)]
async fn pick_server_and_channels(
    client: &reqwest::Client,
    token: &str,
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    prompt_timeout_secs: u32,
) -> Option<(String, ServerChannels)> {
    let bot_guilds = list_bot_guilds(client, token).await;
    if bot_guilds.is_empty() {
        warn!("The bot has no server access at all — cannot configure servers.");
        return None;
    }
    let mut accessible_list = String::new();
    for (i, g) in bot_guilds.iter().enumerate() {
        accessible_list.push_str(&format!("  {}: {}\n", i + 1, g));
    }
    let guild = loop {
        let answer = prompt_for_input(
            engine_write,
            prompt_rx,
            auth_token,
            module_name,
            instance_uuid,
            "Choose the Discord Server",
            &format!(
                "Your bot can currently only access these servers:\n{}\n\n\
                 Enter the NUMBER of a server above (e.g. 1), or paste a Server (Guild) ID directly.",
                accessible_list.trim_end()
            ),
            "Server number or Guild ID",
            PromptKind::String,
            prompt_timeout_secs,
        )
        .await?;
        let trimmed = answer.trim().to_string();
        if let Ok(idx) = trimmed.parse::<usize>() {
            if idx >= 1 && idx <= bot_guilds.len() {
                break bot_guilds[idx - 1].clone();
            }
            warn!("Server number {} is out of range (1..={}).", idx, bot_guilds.len());
        } else if !trimmed.is_empty() && bot_guilds.contains(&trimmed) {
            break trimmed;
        } else if !trimmed.is_empty() {
            warn!("'{}' is not in the bot's accessible servers.", trimmed);
        }
    };

    let channels = prompt_for_input(
        engine_write,
        prompt_rx,
        auth_token,
        module_name,
        instance_uuid,
        "Discord Channels",
        "Which channels should the bot monitor in this server?\n\n\
         Enter channel IDs separated by commas (e.g. 123,456,789).\n\
         Leave empty to monitor ALL channels in the selected server.",
        "Channel IDs (comma-separated, empty = all)",
        PromptKind::String,
        prompt_timeout_secs,
    )
    .await;
    let policy = match channels.as_deref().map(str::trim) {
        Some(tc) if !tc.is_empty() => {
            let ids: Vec<String> = tc.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
            if ids.is_empty() {
                ServerChannels::All
            } else {
                ServerChannels::Some(ids)
            }
        }
        _ => ServerChannels::All,
    };
    Some((guild, policy))
}

/// Validate every configured server + channel against the Discord API, applying
/// the t/i/r/e choices to anything invalid. Returns the final map + the log to
/// surface to the operator.
#[allow(clippy::too_many_arguments)]
async fn resolve_servers(
    client: &reqwest::Client,
    token: &str,
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    prompt_timeout_secs: u32,
    mut servers: std::collections::HashMap<String, ServerChannels>,
) -> (std::collections::HashMap<String, ServerChannels>, String) {
    let bot_guilds = list_bot_guilds(client, token).await;
    let mut log = String::new();

    let (plan, notes) = plan_server_monitoring(&mut servers, &bot_guilds);
    for note in notes {
        log.push_str(&note);
    }

    match plan {
        ServerPlan::NothingToMonitor => {
            log.push_str("no servers configured and the bot is not in any server — idling\n");
        }
        ServerPlan::Pick => {
            if let Some((guild, policy)) = pick_server_and_channels(
                client,
                token,
                engine_write,
                prompt_rx,
                auth_token,
                module_name,
                instance_uuid,
                prompt_timeout_secs,
            )
            .await
            {
                servers.insert(guild.clone(), policy);
                log.push_str(&format!("configured server {} to monitor\n", guild));
            } else {
                log.push_str("no server configured — the bot will idle until configured\n");
            }
        }
        ServerPlan::Resolved { valid, invalid } => {
            let mut final_servers: std::collections::HashMap<String, ServerChannels> = valid;
            // Guilds the bot isn't in: ask the operator, as before.
            for (guild, policy) in invalid {
                let subject = format!("Server {} is NOT in the bot's accessible servers.", guild);
                let choice = prompt_tie_choice(engine_write, prompt_rx, auth_token, module_name, instance_uuid, &subject, prompt_timeout_secs).await;
                match choice {
                    Some(TieChoice::Retry) => {
                        if list_bot_guilds(client, token).await.contains(&guild) {
                            final_servers.insert(guild.clone(), policy.clone());
                            log.push_str(&format!("server {} valid on retry\n", guild));
                        } else {
                            log.push_str(&format!("server {} still invalid on retry — skipped\n", guild));
                        }
                    }
                    Some(TieChoice::Ignore) => {
                        final_servers.insert(guild.clone(), policy.clone());
                        log.push_str(&format!("server {} invalid — ignored (keeping)\n", guild));
                    }
                    Some(TieChoice::Remove) => {
                        log.push_str(&format!("server {} invalid — removed\n", guild));
                    }
                    Some(TieChoice::Edit) => {
                        if let Some(new_id) = prompt_for_input(
                            engine_write, prompt_rx, auth_token, module_name, instance_uuid,
                            "Edit Server ID",
                            "Paste the correct Server (Guild) ID.",
                            "Server ID", PromptKind::String, prompt_timeout_secs,
                        )
                        .await
                        {
                            let t = new_id.trim().to_string();
                            if bot_guilds.contains(&t) {
                                final_servers.insert(t.clone(), policy.clone());
                                log.push_str(&format!("server {} edited to {} (valid)\n", guild, t));
                            } else {
                                log.push_str(&format!("server {} edited to {} — still not in the bot's servers\n", guild, t));
                            }
                        } else {
                            log.push_str(&format!("server {} edit cancelled — removed\n", guild));
                        }
                    }
                    None => {
                        log.push_str(&format!("server {} invalid — skipped (cancelled)\n", guild));
                    }
                }
            }

            // Channel validation for every surviving server, valid or kept.
            let mut resolved: std::collections::HashMap<String, ServerChannels> = Default::default();
            for (guild, policy) in &final_servers {
                match policy {
                    ServerChannels::All => {
                        resolved.insert(guild.clone(), ServerChannels::All);
                        log.push_str(&format!("server {} is valid (all channels)\n", guild));
                    }
                    ServerChannels::Some(chs) => {
                        let channel_ids = guild_channels(client, token, guild).await;
                        let mut kept: Vec<String> = Vec::new();
                        for c in chs {
                            if channel_ids.contains(c) {
                                kept.push(c.clone());
                                log.push_str(&format!("channel {} in {} is valid, testing next\n", c, guild));
                                continue;
                            }
                            // Invalid channel — ask the operator (t/i/r/e).
                            let subject = format!("Channel {} in server {} is NOT a valid channel.", c, guild);
                            let choice = prompt_tie_choice(engine_write, prompt_rx, auth_token, module_name, instance_uuid, &subject, prompt_timeout_secs).await;
                            match choice {
                                Some(TieChoice::Retry) => {
                                    if guild_channels(client, token, guild).await.contains(c) {
                                        kept.push(c.clone());
                                        log.push_str(&format!("channel {} retried — valid\n", c));
                                    } else {
                                        log.push_str(&format!("channel {} retried — still invalid\n", c));
                                    }
                                }
                                Some(TieChoice::Ignore) => {
                                    kept.push(c.clone());
                                    log.push_str(&format!("channel {} invalid — ignored (keeping)\n", c));
                                }
                                Some(TieChoice::Remove) => {
                                    log.push_str(&format!("channel {} invalid — removed\n", c));
                                }
                                Some(TieChoice::Edit) => {
                                    if let Some(new_id) = prompt_for_input(
                                        engine_write, prompt_rx, auth_token, module_name, instance_uuid,
                                        "Edit Channel ID",
                                        "Paste the correct channel ID.",
                                        "Channel ID", PromptKind::String, prompt_timeout_secs,
                                    )
                                    .await
                                    {
                                        let t = new_id.trim().to_string();
                                        if !t.is_empty() && guild_channels(client, token, guild).await.contains(&t) {
                                            kept.push(t.clone());
                                            log.push_str(&format!("channel {} edited to {} (valid)\n", c, t));
                                        } else {
                                            log.push_str(&format!("channel {} edited to {} — still invalid\n", c, t));
                                        }
                                    } else {
                                        log.push_str(&format!("channel {} edit cancelled — removed\n", c));
                                    }
                                }
                                None => {
                                    log.push_str(&format!("channel {} invalid — skipped (cancelled)\n", c));
                                }
                            }
                        }
                        if kept.is_empty() {
                            log.push_str(&format!("server {} has no valid channels — removed\n", guild));
                        } else {
                            resolved.insert(guild.clone(), ServerChannels::Some(kept));
                        }
                    }
                }
            }
            servers = resolved;
        }
    }
    save_adapter_config(token, &servers);
    (servers, log)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Log to stderr WITHOUT ANSI codes: when the TUI pipes module stdout it
    // renders lines into its log window, and raw escape sequences would
    // corrupt the UI.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("Starting Discord Adapter Module...");

    // Load config (non-interactive fast path — the TUI supplies credentials).
    // The bot token is a secret (`.env`); the server→channel map is public
    // (config.json).
    ensure_env_file();
    cockatiel_client::load_env_file(".env");
    let mut bot_token = std::env::var("DISCORD_BOT_TOKEN").unwrap_or_default();
    let mut servers: std::collections::HashMap<String, ServerChannels> = Default::default();
    let tuning = if let Some(saved) = load_adapter_config() {
        if let Some(ref t) = saved.bot_token {
            if !t.is_empty() {
                bot_token = t.clone();
            }
        }
        servers = parse_servers(&saved.servers);
        Tuning::from(&saved)
    } else {
        // No saved config (no bot token) — the tuning knobs still fall back
        // to their hardcoded defaults.
        Tuning::default()
    };

    // Connect to the engine first so prompts can be surfaced to connected UIs
    // before the adapter is configured.
    let cockatiel = CockatielClient::connect("config.json").await?;

    let (write, read) = cockatiel.stream.split();
    let engine_write: Arc<Mutex<WsWriteHalf>> = Arc::new(Mutex::new(write));
    // Shared session identity: the read loop AND the Discord platform send path
    // read the CURRENT token/instance here, so a reconnect (which swaps this)
    // never leaves stale credentials behind.
    let identity: Arc<Mutex<EngineIdentity>> = Arc::new(Mutex::new(EngineIdentity {
        auth: cockatiel.auth_token.clone(),
        instance: cockatiel.instance_uuid7.clone(),
        module: cockatiel.config.module_name.clone(),
    }));

    // Register the mod commands with the engine command system: the engine now
    // parses `!ban` / `!timeout` and routes them back with the parsed Command.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    register_commands(&engine_write, &identity).await;

    // Initial identity, used by the (one-time) setup phase prompts. Runtime
    // sends read the CURRENT identity from the shared handle instead.
    let (auth_token, instance_uuid, module_name) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.instance.clone(), id.module.clone())
    };

    let http = build_http_client(tuning.http_timeout_secs);

    // Send state for the read task, populated once config resolves.
    let send_token: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let send_channels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let send_embed: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    // Channel carrying PromptResponses from the engine to the config loop, so
    // `prompt_for_input` can await the operator's typed answer.
    let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel::<PromptResponse>();

    // Read-loop + engine-session supervisor. When the socket drops the module
    // RECONNECTS (with exponential backoff) instead of going zombie on a dead
    // socket — the old behavior left the platform loop pushing into a dead WS
    // forever. On reconnect the shared write handle + identity are swapped in
    // place and commands re-registered on the fresh session.
    {
        let token_state = send_token.clone();
        let channels_state = send_channels.clone();
        let embed_state = send_embed.clone();
        let http = http.clone();
        let engine_write = engine_write.clone();
        let identity_task = identity.clone();
        let tuning_task = tuning.clone();
        tokio::spawn(async move {
            let mut read = read;
            // Bounded outbound send queue: Discord platform sends are offloaded
            // here so the read loop returns immediately (a stalled REST call must
            // never delay an AuthVerify reply — the engine would sever us as dead
            // past its liveness probe). A fixed worker pool + bounded channel also
            // means a reply flood can't spawn unbounded tasks or pile up memory.
            let (send_tx, send_rx) = mpsc::channel::<DiscordSendJob>(tuning_task.outbound_queue_cap);
            let send_rx = Arc::new(Mutex::new(send_rx));
            for _ in 0..tuning_task.send_worker_count {
                let rx = send_rx.clone();
                tokio::spawn(async move {
                    while let Some(job) = { let mut guard = rx.lock().await; guard.recv().await } {
                        match send_discord_message(&job.client, &job.token, &job.channel_id, &job.msg, job.embed, job.http_timeout_secs).await {
                            Ok(()) => info!("Sent to Discord channel {}: {}", job.channel_id, job.msg),
                            Err(e) => error!("SendToPlatforms failed on {}: {}", job.channel_id, e),
                        }
                    }
                });
            }
            loop {
                // Read until the connection dies.
                while let Some(msg) = read.next().await {
                    let Ok(WsMessage::Binary(data)) = msg else { continue };
                    let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                    let Some(payload) = container.payload else { continue };
                    // Use the CURRENT session identity (a reconnect swaps it).
                    let (auth, instance, module) = {
                        let id = identity_task.lock().await;
                        (id.auth.clone(), id.instance.clone(), id.module.clone())
                    };
                    match payload {
                        // Answer the engine's liveness probe with our auth token
                        // so a quiet period never severs us as unresponsive.
                        ModulePayload::AuthVerify(_) => {
                            let reply = ContainerForEngine {
                                version: 2,
                                auth_token: auth.clone(),
                                module_name: module.clone(),
                                module_instance_uuid7: instance.clone(),
                                payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                    cur_auth: auth.clone(),
                                })),
                            };
                            let mut buf = Vec::new();
                            if reply.encode(&mut buf).is_ok() {
                                let mut w = engine_write.lock().await;
                                let _ = w.send(WsMessage::Binary(buf)).await;
                            }
                        }
                        ModulePayload::SendToPlatforms(send) => {
                            let token = token_state.lock().await.clone();
                            let channels = channels_state.lock().await.clone();
                            let embed = *embed_state.lock().await;
                            // Target a single channel when the sender specified one
                            // (e.g. engine !help / invalid-command replies); else
                            // send to every monitored channel.
                            let targets: Vec<String> = if !send.channel_id.is_empty() {
                                channels.iter().filter(|c| **c == send.channel_id).cloned().collect()
                            } else {
                                channels.clone()
                            };
                            if targets.is_empty() {
                                warn!("SendToPlatforms received but no matching channel configured.");
                                continue;
                            }
                            // Offload every send to the worker pool so the read
                            // loop returns immediately (AuthVerify stays responsive).
                            for ch in targets {
                                let job = DiscordSendJob {
                                    client: http.clone(),
                                    token: token.clone(),
                                    channel_id: ch.clone(),
                                    msg: send.msg.clone(),
                                    embed,
                                    http_timeout_secs: tuning_task.http_timeout_secs,
                                };
                                if send_tx.try_send(job).is_err() {
                                    warn!("SendToPlatforms queue full — dropping reply to channel {}", ch);
                                }
                            }
                        }
                        ModulePayload::PromptResponse(resp) => {
                            // Forward operator answers to the awaiting prompt.
                            let _ = prompt_tx.send(resp);
                        }
                        // Routed chat command: the engine parsed `!ban` / `!timeout`
                        // and delivered it here with the parsed Command attached.
                        ModulePayload::MessagePreProcess(pre) => {
                            // Receipt ping: confirm delivery to the engine
                            // IMMEDIATELY (pure ack, separate from the stage
                            // echo below) so the engine doesn't resend.
                            if !pre.message_uuid7.is_empty() {
                                let receipt = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth.clone(),
                                    module_name: module.clone(),
                                    module_instance_uuid7: instance.clone(),
                                    payload: Some(EnginePayload::MessageAck(MessageAck {
                                        message_uuid7: pre.message_uuid7.clone(),
                                    })),
                                };
                                let mut rbuf = Vec::new();
                                if receipt.encode(&mut rbuf).is_ok() {
                                    let mut w = engine_write.lock().await;
                                    let _ = w.send(WsMessage::Binary(rbuf)).await;
                                }
                            }
                            let uuid = pre.message_uuid7.clone();
                            let Some(chat) = pre.raw_message else { continue };
                            let Some(cmd) = chat.command.as_ref() else { continue };
                            if cmd.command_name != "ban" && cmd.command_name != "timeout" {
                                continue;
                            }
                            let author = chat
                                .user_data
                                .as_ref()
                                .map(|u| u.username.clone())
                                .unwrap_or_default();
                            if let Some((qid, payload)) = build_mod_query(
                                &cmd.command_name,
                                &chat.raw_message,
                                &author,
                                tuning_task.default_timeout_secs,
                            ) {
                                let query = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth.clone(),
                                    module_name: module.clone(),
                                    module_instance_uuid7: instance.clone(),
                                    payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                                        query_id: qid,
                                        sql: payload.to_string(),
                                        params: vec![],
                                    })),
                                };
                                let mut qbuf = Vec::new();
                                if query.encode(&mut qbuf).is_ok() {
                                    let mut w = engine_write.lock().await;
                                    let _ = w.send(WsMessage::Binary(qbuf)).await;
                                }
                            }
                            // ACK the pre_process stage: echo the raw ChatMessage
                            // back with the SAME message_uuid7 so the engine marks
                            // this message pre-processed. Without this the command
                            // message strands in the pipeline until the engine's
                            // timeout sweep.
                            let ack = ContainerForEngine {
                                version: 2,
                                auth_token: auth.clone(),
                                module_name: module.clone(),
                                module_instance_uuid7: instance.clone(),
                                payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
                                    audio: vec![],
                                    audio_type: String::new(),
                                    message_uuid7: uuid,
                                    raw_message: Some(chat.clone()),
                                })),
                            };
                            let mut abuf = Vec::new();
                            if ack.encode(&mut abuf).is_ok() {
                                let mut w = engine_write.lock().await;
                                let _ = w.send(WsMessage::Binary(abuf)).await;
                            }
                        }
                        _ => {}
                    }
                }

                // The engine connection dropped — reconnect with backoff instead of
                // leaving the platform loop pushing into a dead socket.
                info!("Engine disconnected — reconnecting...");
                let mut backoff = tuning_task.reconnect_base_secs;
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
                    match CockatielClient::connect("config.json").await {
                        Ok(conn) => {
                            info!("Reconnected to engine");
                            let (w, r) = conn.stream.split();
                            *engine_write.lock().await = w;
                            *identity_task.lock().await = EngineIdentity {
                                auth: conn.auth_token,
                                instance: conn.instance_uuid7,
                                module: conn.config.module_name,
                            };
                            // The engine forgets a session's commands when the
                            // socket drops — re-register on the fresh session.
                            register_commands(&engine_write, &identity_task).await;
                            read = r;
                            break;
                        }
                        Err(e) => {
                            error!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                            backoff = (backoff * 2).min(tuning_task.reconnect_max_secs);
                        }
                    }
                }
            }
        });
    }

    // ── Linear setup phase ────────────────────────────────────────────────
    // Auth: prompt for a token if missing, verify it against the Discord API.
    let http_setup = build_http_client(tuning.http_timeout_secs);
    let bot_token = match resolve_auth(
        &http_setup,
        &engine_write,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
        REST_API,
        bot_token,
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            error!("{}", e);
            std::process::exit(1);
        }
    };
    // Persist only a token Discord has actually accepted — a rejected one must
    // never be left behind in `.env` for the next start to retry forever.
    cockatiel_client::write_env_file(".env", &[("DISCORD_BOT_TOKEN", &bot_token)]);

    // Servers: validate every configured server + channel (t/i/r/e on invalid),
    // or pick a server + channels when none are configured.
    let (servers, setup_log) = resolve_servers(
        &http_setup,
        &bot_token,
        &engine_write,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
        tuning.prompt_timeout_secs,
        servers,
    )
    .await;

    // Surface the setup summary to the operator (the accumulated log).
    if !setup_log.trim().is_empty() {
        let log = ContainerForEngine {
            version: 2,
            auth_token: auth_token.clone(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_uuid.clone(),
            payload: Some(EnginePayload::Log(cockatiel_client::proto::Log {
                log: format!("[discord-adapter] setup:\n{}", setup_log.trim_end()),
                blob: vec![],
            })),
        };
        let mut lbuf = Vec::new();
        if log.encode(&mut lbuf).is_ok() {
            let mut w = engine_write.lock().await;
            let _ = w.send(WsMessage::Binary(lbuf)).await;
        }
    }

    // Publish the resolved token/channels to the read task. `*` servers are
    // expanded to their text channels so engine replies are never dropped.
    *send_token.lock().await = bot_token.clone();
    *send_channels.lock().await = resolve_send_channels(&http, &bot_token, &servers).await;
    let embed_sends = load_adapter_config().map(|c| c.embed_sends).unwrap_or(false);
    *send_embed.lock().await = embed_sends;

    // Member-count monitor: every poll interval, report each configured guild's
    // approximate member count to the engine via ChannelStats (the Discord
    // stand-in for a viewer count). Runs alongside the gateway task.
    {
        let http_monitor = http.clone();
        let token_monitor = bot_token.clone();
        let guilds: Vec<String> = servers.keys().cloned().collect();
        let engine_write_monitor = engine_write.clone();
        let identity_monitor = identity.clone();
        // 30s matches the other adapters' default stream-poll intervals.
        let interval_secs = 30u64;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // burn the immediate first tick
            loop {
                interval.tick().await;
                for guild in &guilds {
                    if let Some(count) =
                        guild_member_count(&http_monitor, &token_monitor, guild).await
                    {
                        push_channel_stats(
                            &engine_write_monitor,
                            &identity_monitor,
                            "discord",
                            guild,
                            count,
                            true,
                            "",
                        )
                        .await;
                    }
                }
            }
        });
    }

    // Discord gateway task — receive-only (all setup/validation happened above).
    info!("Connecting to Discord gateway...");
    run_discord_gateway(
        http,
        bot_token,
        servers,
        engine_write,
        send_channels,
        identity,
        &tuning,
    )
    .await;

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{merge_send_channels, parse_servers, ServerChannels, extract_image_urls, build_mod_query, parse_tie_choice, TieChoice, DiscordAdapterConfig, ensure_env_file_at, ENV_KEYS, bot_auth_value, build_http_client, check_discord_auth, retry_after_secs_from_body, describe_auth_failure, auth_retry_action, transient_backoff_secs, should_reset_rejection_budget, parse_servers_value, expand_guild_wildcard, plan_server_monitoring, ServerPlan, AuthAction, AuthFailure, MAX_AUTH_ATTEMPTS, guild_member_count_from_body};
    use serde_json::json;
    use wiremock::matchers::{header_regex, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn tie_choice_parses_all_options() {
        assert_eq!(parse_tie_choice("t"), Some(TieChoice::Retry));
        assert_eq!(parse_tie_choice("try again"), Some(TieChoice::Retry));
        assert_eq!(parse_tie_choice("i"), Some(TieChoice::Ignore));
        assert_eq!(parse_tie_choice("r"), Some(TieChoice::Remove));
        assert_eq!(parse_tie_choice("e"), Some(TieChoice::Edit));
        assert_eq!(parse_tie_choice("EDIT"), Some(TieChoice::Edit));
        assert_eq!(parse_tie_choice("x"), None);
        assert_eq!(parse_tie_choice(""), None);
    }

    #[test]
    fn parses_multi_server_config() {
        let input: std::collections::HashMap<String, Vec<String>> = serde_json::from_value(json!({
            "1543036894273732640": ["ch1", "ch2"],
            "1543076319296753804": ["*"],
            "1543127132421754922": [],
        })).unwrap();
        let servers = parse_servers(&input);
        assert_eq!(servers.len(), 3);
        assert!(matches!(servers.get("1543036894273732640"), Some(ServerChannels::Some(chs)) if chs == &vec!["ch1".to_string(), "ch2".to_string()]));
        assert!(matches!(servers.get("1543076319296753804"), Some(ServerChannels::All)));
        // An empty channel list means all channels.
        assert!(matches!(servers.get("1543127132421754922"), Some(ServerChannels::All)));
    }

    #[test]
    fn merge_send_channels_unions_and_resolves_all() {
        let input: std::collections::HashMap<String, Vec<String>> = serde_json::from_value(json!({
            "g1": ["ch1", "ch2"],
            "g2": ["*"],
            "g3": ["ch3"],
        })).unwrap();
        let servers = parse_servers(&input);
        // "*" (ServerChannels::All) is resolved to the guild's text channels
        // instead of being silently dropped.
        let mut all = std::collections::HashMap::new();
        all.insert("g2".to_string(), vec!["ga".to_string(), "gb".to_string()]);
        let send = merge_send_channels(&servers, &all);
        assert_eq!(
            send,
            vec![
                "ch1".to_string(),
                "ch2".to_string(),
                "ch3".to_string(),
                "ga".to_string(),
                "gb".to_string(),
            ]
        );
    }

    #[test]
    fn merge_send_channels_dedups_and_handles_missing_all() {
        let input: std::collections::HashMap<String, Vec<String>> = serde_json::from_value(json!({
            "g1": ["ch1", "ch1", "ch2"],
        })).unwrap();
        let servers = parse_servers(&input);
        let send = merge_send_channels(&servers, &std::collections::HashMap::new());
        assert_eq!(send, vec!["ch1".to_string(), "ch2".to_string()]);
    }

    #[test]
    fn extracts_attachment_and_embed_images() {
        let msg = json!({
            "attachments": [
                { "url": "https://cdn.discordapp.com/attachments/1/2/photo.png?ex=123&is=456" },
                { "url": "https://cdn.discordapp.com/attachments/1/2/clip.mp4" },
            ],
            "embeds": [
                { "image": { "url": "https://media.example.com/banner.webp" } },
                { "thumbnail": { "url": "https://cdn.discordapp.com/attachments/1/2/thumb.jpg" } },
                { "title": "no image here" },
            ],
        });
        let urls = extract_image_urls(&msg);
        assert_eq!(urls.len(), 3);
        assert!(urls[0].starts_with("https://cdn.discordapp.com/attachments/1/2/photo.png"));
        assert_eq!(urls[1], "https://media.example.com/banner.webp");
        assert_eq!(urls[2], "https://cdn.discordapp.com/attachments/1/2/thumb.jpg");
    }

    #[test]
    fn empty_when_no_images() {
        let msg = json!({
            "attachments": [],
            "embeds": [{ "title": "text only" }],
            "content": "hello"
        });
        assert!(extract_image_urls(&msg).is_empty());
    }

    #[test]
    fn ban_with_duration_routes_to_timeout() {
        // Discord bans are permanent — `!ban @user -d 300` must become a
        // mod_timeout so the engine can unban later.
        let (qid, payload) = build_mod_query("ban", "!ban @user -d 300 spamming", "mod", 300).unwrap();
        assert_eq!(qid, "mod_timeout");
        assert_eq!(payload["duration_secs"], 300);
        assert_eq!(payload["handle"], "user");
        assert_eq!(payload["reason"], "spamming");
        assert_eq!(payload["actor"]["handle"], "mod");

        // Plain ban stays a permanent mod_ban.
        let (qid, payload) = build_mod_query("ban", "!ban @user being awful", "mod", 300).unwrap();
        assert_eq!(qid, "mod_ban");
        assert_eq!(payload["reason"], "being awful");

        // !timeout with no explicit duration uses the configured default.
        let (qid, payload) = build_mod_query("timeout", "!timeout @user spam", "mod", 300).unwrap();
        assert_eq!(qid, "mod_timeout");
        assert_eq!(payload["duration_secs"], 300);
    }

    #[test]
    fn config_tuning_defaults() {
        // A module_specific section WITHOUT tuning keys must fall back to the
        // hardcoded defaults for every knob.
        let cfg: DiscordAdapterConfig =
            serde_json::from_value(json!({ "servers": {} })).unwrap();
        assert_eq!(cfg.default_timeout_secs, 300);
        assert_eq!(cfg.http_timeout_secs, 15);
        assert_eq!(cfg.gateway_auth_backoff_base_secs, 1);
        assert_eq!(cfg.gateway_auth_backoff_max_secs, 30);
        assert_eq!(cfg.gateway_connect_retry_secs, 5);
        assert_eq!(cfg.gateway_reconnect_delay_secs, 5);
        assert_eq!(cfg.outbound_queue_cap, 64);
        assert_eq!(cfg.send_worker_count, 4);
        assert_eq!(cfg.reconnect_base_secs, 1);
        assert_eq!(cfg.reconnect_max_secs, 30);
        assert_eq!(cfg.prompt_timeout_secs, 300);
        // Explicit keys override the defaults.
        let cfg: DiscordAdapterConfig =
            serde_json::from_value(json!({ "http_timeout_secs": 9, "send_worker_count": 2 })).unwrap();
        assert_eq!(cfg.http_timeout_secs, 9);
        assert_eq!(cfg.send_worker_count, 2);
        assert_eq!(cfg.prompt_timeout_secs, 300);
    }

    #[test]
    fn ensure_env_file_creates_and_merges_placeholders() {
        let dir = std::env::temp_dir().join(format!("discord_env_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".env");
        let _ = std::fs::remove_file(&path);

        // Missing file: created with every expected key as an EMPTY placeholder.
        ensure_env_file_at(&path);
        let created = std::fs::read_to_string(&path).unwrap();
        for key in ENV_KEYS {
            assert!(created.lines().any(|l| l == format!("{}=", key)), "{} missing", key);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, ".env must be owner-only");
        }

        // Existing non-empty values preserved; missing keys appended once.
        let first = format!("{}=", ENV_KEYS[0]);
        std::fs::write(&path, format!("{}{}\nSOME_OTHER=keep\n", first, "abc")).unwrap();
        ensure_env_file_at(&path);
        let merged = std::fs::read_to_string(&path).unwrap();
        assert!(merged.contains(&format!("{}{}", first, "abc")), "existing value overwritten");
        assert!(merged.contains("SOME_OTHER=keep"), "unrelated key dropped");
        assert_eq!(merged.matches(&first).count(), 1, "existing key line duplicated");
        for key in ENV_KEYS {
            assert!(
                merged.lines().any(|l| l.starts_with(&format!("{}=", key))),
                "{} missing after merge",
                key
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Bot-token auth scheme (regression: "Bearer" is a USER scheme) ────

    #[test]
    fn bot_auth_value_uses_the_bot_scheme() {
        assert_eq!(bot_auth_value("abc"), "Bot abc");
        assert!(!bot_auth_value("abc").contains("Bearer"));
        assert_eq!(bot_auth_value(""), "Bot ");
    }

    #[tokio::test]
    async fn auth_check_sends_a_bot_authorization_header() {
        // A `Bearer` scheme made Discord answer 401 to a perfectly good token,
        // which sent setup into an endless "token rejected" re-prompt loop. The
        // matcher alone makes this fail if the scheme ever regresses.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/users/@me"))
            .and(header_regex("authorization", "^Bot tok-123$"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client = build_http_client(5);
        assert!(check_discord_auth(&client, &server.uri(), "tok-123").await.is_ok());

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let auth = requests[0]
            .headers
            .get(&"authorization".parse().unwrap())
            .expect("no Authorization header was sent")
            .as_str()
            .to_string();
        assert!(auth.starts_with("Bot "), "expected a Bot scheme, got {:?}", auth);
        assert_eq!(auth, "Bot tok-123");
    }

    // ── check_discord_auth failure mapping ──────────────────────────────

    async fn auth_check_against(response: ResponseTemplate) -> Result<(), AuthFailure> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/users/@me"))
            .respond_with(response)
            .mount(&server)
            .await;
        let client = build_http_client(5);
        check_discord_auth(&client, &server.uri(), "tok-123").await
    }

    #[tokio::test]
    async fn auth_check_maps_success_to_ok() {
        assert!(auth_check_against(ResponseTemplate::new(200)).await.is_ok());
        assert!(auth_check_against(ResponseTemplate::new(204)).await.is_ok());
    }

    #[tokio::test]
    async fn auth_check_maps_401_and_403_to_unauthorized() {
        assert_eq!(
            auth_check_against(ResponseTemplate::new(401)).await,
            Err(AuthFailure::Unauthorized { status: 401 })
        );
        assert_eq!(
            auth_check_against(ResponseTemplate::new(403)).await,
            Err(AuthFailure::Unauthorized { status: 403 })
        );
    }

    #[tokio::test]
    async fn auth_check_maps_429_to_rate_limited_with_body_delay() {
        // Discord reports the delay as JSON in the BODY for bot tokens.
        let resp = ResponseTemplate::new(429)
            .set_body_string(json!({ "message": "You are being rate limited.", "retry_after": 1.234, "global": false }).to_string());
        assert_eq!(
            auth_check_against(resp).await,
            Err(AuthFailure::RateLimited { retry_after_secs: 2 })
        );
    }

    #[tokio::test]
    async fn auth_check_falls_back_when_the_429_body_is_unusable() {
        for body in [r#"{"message":"nope"}"#, "not json at all", ""] {
            let resp = ResponseTemplate::new(429).set_body_string(body.to_string());
            assert_eq!(
                auth_check_against(resp).await,
                Err(AuthFailure::RateLimited { retry_after_secs: 5 }),
                "body {:?} should fall back to 5s",
                body
            );
        }
    }

    #[tokio::test]
    async fn auth_check_maps_server_errors_to_server() {
        assert_eq!(
            auth_check_against(ResponseTemplate::new(503)).await,
            Err(AuthFailure::Server { status: 503 })
        );
        assert_eq!(
            auth_check_against(ResponseTemplate::new(418)).await,
            Err(AuthFailure::Server { status: 418 })
        );
    }

    #[tokio::test]
    async fn auth_check_reports_unreachable_discord_as_transport() {
        // Nothing listens on port 1 — the check must surface a transport error,
        // never a bogus "your token is wrong".
        let client = build_http_client(1);
        let err = check_discord_auth(&client, "http://127.0.0.1:1", "tok-123")
            .await
            .expect_err("an unreachable host cannot authenticate");
        assert!(matches!(err, AuthFailure::Transport { .. }), "got {:?}", err);
        let AuthFailure::Transport { detail } = &err else {
            panic!("expected a transport failure, got {:?}", err)
        };
        assert!(!detail.is_empty(), "the transport detail must survive for the log");
    }

    #[test]
    fn retry_after_rounds_up_with_a_floor() {
        assert_eq!(retry_after_secs_from_body(Some(r#"{"retry_after":1.234}"#)), 2);
        assert_eq!(retry_after_secs_from_body(Some(r#"{"retry_after":1.0}"#)), 1);
        assert_eq!(retry_after_secs_from_body(Some(r#"{"retry_after":0.01}"#)), 1);
        assert_eq!(retry_after_secs_from_body(Some(r#"{"retry_after":7.5}"#)), 8);
        assert_eq!(retry_after_secs_from_body(Some(r#"{"retry_after":30}"#)), 30);
        // Absent / unparseable / wrong type all fall back to 5s.
        assert_eq!(retry_after_secs_from_body(Some(r#"{"global":true}"#)), 5);
        assert_eq!(retry_after_secs_from_body(Some(r#"{"retry_after":"soon"}"#)), 5);
        assert_eq!(retry_after_secs_from_body(Some("garbage")), 5);
        assert_eq!(retry_after_secs_from_body(Some("")), 5);
        assert_eq!(retry_after_secs_from_body(None), 5);
    }

    #[test]
    fn auth_failures_describe_the_real_cause() {
        let unauthorized = describe_auth_failure(&AuthFailure::Unauthorized { status: 401 });
        assert!(unauthorized.contains("401"), "got {:?}", unauthorized);
        assert!(unauthorized.to_lowercase().contains("revoked"), "got {:?}", unauthorized);
        let transport = describe_auth_failure(&AuthFailure::Transport { detail: "dns error".to_string() });
        assert!(transport.contains("could not reach discord.com"), "got {:?}", transport);
        assert!(transport.contains("dns error"), "got {:?}", transport);
    }

    // ── Retry policy: what clears the token, what counts as an attempt ───

    #[test]
    fn only_unauthorized_clears_the_token() {
        let unauthorized = AuthFailure::Unauthorized { status: 401 };
        assert_eq!(auth_retry_action(&unauthorized, 0, 0), AuthAction::ClearToken);
        assert_eq!(
            auth_retry_action(&unauthorized, MAX_AUTH_ATTEMPTS - 2, 0),
            AuthAction::ClearToken
        );
        // A rate limit, a 5xx, and an unreachable network all keep the token.
        assert_eq!(
            auth_retry_action(&AuthFailure::RateLimited { retry_after_secs: 3 }, 0, 0),
            AuthAction::RetryAfter(3)
        );
        assert_eq!(
            auth_retry_action(&AuthFailure::Server { status: 503 }, 0, 0),
            AuthAction::RetryAfter(2)
        );
        assert_eq!(
            auth_retry_action(&AuthFailure::Transport { detail: "timeout".to_string() }, 0, 0),
            AuthAction::RetryAfter(2)
        );
    }

    #[test]
    fn rejections_are_capped_so_the_supervisor_can_restart() {
        let unauthorized = AuthFailure::Unauthorized { status: 401 };
        assert_eq!(
            auth_retry_action(&unauthorized, MAX_AUTH_ATTEMPTS - 1, 0),
            AuthAction::GiveUp
        );
        // A transient failure never advances the cap, however long it repeats.
        let transient = AuthFailure::Server { status: 503 };
        for prior_rejections in 0..MAX_AUTH_ATTEMPTS {
            assert!(
                !matches!(
                    auth_retry_action(&transient, prior_rejections, 9),
                    AuthAction::GiveUp
                ),
                "a 5xx must never end setup, even after {} rejections",
                prior_rejections
            );
        }
    }

    #[test]
    fn transient_backoff_doubles_and_caps() {
        assert_eq!(transient_backoff_secs(0), 2);
        assert_eq!(transient_backoff_secs(1), 4);
        assert_eq!(transient_backoff_secs(2), 8);
        assert_eq!(transient_backoff_secs(3), 16);
        assert_eq!(transient_backoff_secs(4), 30);
        assert_eq!(transient_backoff_secs(50), 30);
        assert_eq!(transient_backoff_secs(u32::MAX), 30);
        // The delay grows with consecutive transient failures, never past 30s.
        let delays: Vec<u64> = (0..6).map(transient_backoff_secs).collect();
        assert_eq!(delays, vec![2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn a_different_token_earns_a_fresh_budget_but_the_same_one_does_not() {
        // First ever submission — nothing rejected yet, so it is a fresh start.
        assert!(should_reset_rejection_budget(None, "tok-a"));
        // A DIFFERENT token is a new attempt: it must get the full budget, or a
        // single bad stored token would burn every later submission's budget.
        assert!(should_reset_rejection_budget(Some("tok-a"), "tok-b"));
        // Re-pasting the token Discord just rejected is NOT a new attempt — the
        // counter must keep climbing so the setup terminates.
        assert!(!should_reset_rejection_budget(Some("tok-a"), "tok-a"));
    }

    #[test]
    fn three_identical_rejections_end_the_setup_but_three_distinct_ones_do_not() {
        // Walk the real policy the way resolve_auth drives it: a submission that
        // differs from the last rejected token resets, an identical one does not.
        let mut rejections = 0u32;
        let mut last_rejected: Option<&str> = None;
        for attempt in ["tok-a", "tok-a", "tok-b"] {
            if should_reset_rejection_budget(last_rejected, attempt) {
                rejections = 0;
            }
            let action = auth_retry_action(
                &AuthFailure::Unauthorized { status: 401 },
                rejections,
                0,
            );
            rejections += 1;
            last_rejected = Some(attempt);
            if attempt == "tok-b" {
                assert!(
                    !matches!(action, AuthAction::GiveUp),
                    "a different token after 2 rejections must still be retried, got {:?}",
                    action
                );
            }
        }
        // The same token submitted MAX_AUTH_ATTEMPTS times does give up.
        let mut rejections = 0u32;
        let mut gave_up = false;
        for _ in 0..MAX_AUTH_ATTEMPTS {
            if should_reset_rejection_budget(last_rejected, "tok-c") {
                rejections = 0;
            }
            if matches!(
                auth_retry_action(&AuthFailure::Unauthorized { status: 401 }, rejections, 0),
                AuthAction::GiveUp
            ) {
                gave_up = true;
            }
            rejections += 1;
            last_rejected = Some("tok-c");
        }
        assert!(gave_up, "re-submitting one rejected token {} times must end setup", MAX_AUTH_ATTEMPTS);
    }

    // ── Server config: wildcards + the object/array datatype split ─────────

    #[test]
    fn a_literal_star_guild_key_matches_no_real_guild() {
        // The receive filter is `servers.get(<real guild id from the payload>)`.
        // Documenting the exact defect: a literal "*" key can never match, so
        // every MESSAGE_CREATE is dropped while the bot sits in real servers.
        let servers: std::collections::HashMap<String, ServerChannels> =
            parse_servers(&std::collections::HashMap::from([(
                "*".to_string(),
                vec!["*".to_string()],
            )]));
        let real_guild_from_payload = "1543036894273732640";
        assert!(
            !servers.contains_key(real_guild_from_payload),
            "a literal '*' key must not match a real guild id (hence the wildcard expansion in resolve_servers)"
        );
    }

    #[test]
    fn a_bare_guild_means_all_channels() {
        let parsed = parse_servers(&std::collections::HashMap::from([(
            "1543036894273732640".to_string(),
            vec![],
        )]));
        assert!(matches!(
            parsed.get("1543036894273732640"),
            Some(ServerChannels::All)
        ));
        let star = parse_servers(&std::collections::HashMap::from([(
            "g".to_string(),
            vec!["*".to_string()],
        )]));
        assert!(matches!(star.get("g"), Some(ServerChannels::All)));
    }

    #[test]
    fn servers_parses_from_every_shape_its_writers_produce() {
        // The module's own save_adapter_config writes the OBJECT form...
        let object = serde_json::json!({ "1543036894273732640": ["general"] });
        let parsed = parse_servers_value(&object);
        assert_eq!(parsed.get("1543036894273732640"), Some(&vec!["general".to_string()]));

        // ...and the engine's save_module_credentials writes a `list: true`
        // credential as an ARRAY of "guild=channels" lines. This is the shape
        // that used to fail deserialisation and silently empty the map.
        let array = serde_json::json!([
            "1543036894273732640=general,test1",
            "1543076319296753804",
            "*"
        ]);
        let parsed = parse_servers_value(&array);
        assert_eq!(
            parsed.get("1543036894273732640"),
            Some(&vec!["general".to_string(), "test1".to_string()])
        );
        assert_eq!(parsed.get("1543076319296753804"), Some(&vec![]));
        assert_eq!(parsed.get("*"), Some(&vec![]));

        // A single hand-edited string with newlines.
        let string = serde_json::json!("a=1\nb=2,3");
        let parsed = parse_servers_value(&string);
        assert_eq!(parsed.get("a"), Some(&vec!["1".to_string()]));
        assert_eq!(parsed.get("b"), Some(&vec!["2".to_string(), "3".to_string()]));

        // Garbage must not panic and must not invent servers.
        assert!(parse_servers_value(&serde_json::json!(7)).is_empty());
        assert!(parse_servers_value(&serde_json::json!(null)).is_empty());
        assert!(parse_servers_value(&serde_json::json!(["", "   ", "=", "=1"])).is_empty());
    }

    #[test]
    fn the_star_guild_wildcard_is_the_key_the_expansion_looks_for() {
        // Both writer forms must produce the bare "*" key that resolve_servers
        // expands, or the expansion silently does nothing.
        let from_array = parse_servers_value(&serde_json::json!(["*"]));
        assert!(from_array.contains_key("*"), "array form must yield the '*' key");
        let from_object = parse_servers_value(&serde_json::json!({ "*": ["*"] }));
        assert!(from_object.contains_key("*"), "object form must yield the '*' key");
    }

    #[test]
    fn the_star_guild_wildcard_expands_to_every_server_the_bot_is_in() {
        // Real guild ids from GET /users/@me/guilds for this bot.
        let bot_guilds: Vec<String> = [
            "1543036894273732640",
            "1543076319296753804",
            "1543078575651946520",
            "1543127132421754922",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        // Exactly what config.json holds today: { "*": ["*"] }
        let mut servers = parse_servers(&parse_servers_value(
            &serde_json::json!({ "*": ["*"] }),
        ));
        assert!(servers.contains_key("*"), "precondition: the '*' key is present");

        let line = expand_guild_wildcard(&mut servers, &bot_guilds).expect("expansion ran");
        assert!(!servers.contains_key("*"), "the '*' key must be gone after expansion");

        // THE FIX: every real guild id a MESSAGE_CREATE payload can carry must
        // now be present, so the literal `servers.get(guild_id)` receive filter
        // stops dropping every message.
        for guild in &bot_guilds {
            assert!(
                servers.contains_key(guild),
                "guild {} must be monitored after expansion, got {:?}",
                guild,
                servers.keys().collect::<Vec<_>>()
            );
            assert!(matches!(servers.get(guild), Some(ServerChannels::All)));
        }
        assert_eq!(servers.len(), bot_guilds.len());
        assert!(line.contains("expanded to 4 server(s)"), "got {:?}", line);
    }

    #[test]
    fn wildcard_expansion_never_silently_monitors_nothing() {
        // `*=ch1,ch2` cannot apply across servers (channel ids are per-server),
        // so it degrades to All and says so.
        let mut servers = parse_servers(&parse_servers_value(&serde_json::json!({ "*": ["ch1"] })));
        let guilds = vec!["g1".to_string(), "g2".to_string()];
        let line = expand_guild_wildcard(&mut servers, &guilds).expect("expansion ran");
        assert!(matches!(servers.get("g1"), Some(ServerChannels::All)));
        assert!(matches!(servers.get("g2"), Some(ServerChannels::All)));
        assert!(line.contains("cannot apply across servers"), "got {:?}", line);

        // A bot in no servers must say so instead of pretending to monitor.
        let mut servers = parse_servers(&parse_servers_value(&serde_json::json!({ "*": ["*"] })));
        let line = expand_guild_wildcard(&mut servers, &[]).expect("expansion ran");
        assert!(servers.is_empty());
        assert!(line.contains("0 servers"), "got {:?}", line);

        // No wildcard key → nothing to do.
        let mut servers = parse_servers(&parse_servers_value(
            &serde_json::json!({ "g1": ["*"] }),
        ));
        assert!(expand_guild_wildcard(&mut servers, &["g1".to_string()]).is_none());
        assert_eq!(servers.len(), 1);
    }

    #[test]
    fn resolve_auth_never_persists_the_token_itself() {
        // The write belongs to the caller, after validation — so a rejected
        // token can never be left behind in `.env` for the next start to
        // re-validate (and re-prompt) forever.
        let src = include_str!("main.rs");
        let start = src
            .find("async fn resolve_auth(")
            .expect("resolve_auth not found in main.rs");
        let end = start
            + src[start..]
                .find("\n}\n")
                .expect("end of resolve_auth not found in main.rs");
        let body = &src[start..end];
        assert!(
            !body.contains("write_env_file"),
            "resolve_auth must not write .env"
        );
        assert!(!body.contains(".env"), "resolve_auth must not touch .env");
    }

    #[test]
    fn resolve_servers_actually_calls_the_server_plan() {
        // A behavioural test can't cover the WIRING (resolve_servers needs a live
        // engine socket), and deleting the call site silently disables the whole
        // fix while every behavioural test still passes. Assert the wiring
        // structurally so that regression fails here instead of in production.
        let src = include_str!("main.rs");
        let start = src
            .find("async fn resolve_servers(")
            .expect("resolve_servers not found in main.rs");
        let end = start
            + src[start..]
                .find("\nasync fn ")
                .expect("end of resolve_servers not found in main.rs");
        assert!(
            src[start..end].contains("plan_server_monitoring("),
            "resolve_servers must delegate the server plan (which expands the '*' wildcard)"
        );
        // The wildcard must be expanded before guilds are classified, or '*'
        // trips the "not in the bot's servers" prompt again. That ordering now
        // lives inside the plan fn, so assert it there.
        let pstart = src
            .find("fn plan_server_monitoring(")
            .expect("plan_server_monitoring not found in main.rs");
        let pend = pstart
            + src[pstart..]
                .find("\n}\n")
                .expect("end of plan_server_monitoring not found in main.rs");
        let plan_body = &src[pstart..pend];
        let expand_at = plan_body
            .find("expand_guild_wildcard(")
            .expect("the plan must expand the '*' wildcard");
        let classify_at = plan_body
            .find("bot_guilds.contains(guild)")
            .expect("the plan must classify guilds against the bot's guilds");
        assert!(
            expand_at < classify_at,
            "the '*' wildcard must be expanded before guilds are classified"
        );
    }

    // ── Server planning: the decision layer, with no I/O ─────────────────

    #[test]
    fn a_wildcard_only_config_plans_to_monitor_every_real_guild() {
        // THE regression: config.json = { "*": ["*"] } with the bot in 4 guilds.
        let guilds: Vec<String> = [
            "1543036894273732640",
            "1543076319296753804",
            "1543078575651946520",
            "1543127132421754922",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut servers = parse_servers(&parse_servers_value(&serde_json::json!({ "*": ["*"] })));
        let (plan, notes) = plan_server_monitoring(&mut servers, &guilds);
        match plan {
            ServerPlan::Resolved { valid, invalid } => {
                assert!(invalid.is_empty(), "no guild should be invalid, got {:?}", invalid);
                assert_eq!(valid.len(), 4);
                for g in &guilds {
                    assert!(valid.contains_key(g), "{} must be monitored", g);
                }
            }
            other => panic!("expected Resolved, got {:?}", other),
        }
        assert!(
            notes.iter().any(|n| n.contains("expanded to 4 server(s)")),
            "got {:?}",
            notes
        );
    }

    #[test]
    fn planning_splits_guilds_the_bot_is_in_from_guilds_it_is_not() {
        let guilds = vec!["g1".to_string(), "g2".to_string()];
        let mut servers = parse_servers(&parse_servers_value(&serde_json::json!({
            "g1": ["*"],
            "g3": ["*"],
            "g2": ["chan-a"]
        })));
        let (plan, _) = plan_server_monitoring(&mut servers, &guilds);
        match plan {
            ServerPlan::Resolved { valid, invalid } => {
                assert_eq!(valid.len(), 2);
                assert!(valid.contains_key("g1"));
                assert!(
                    matches!(valid.get("g2"), Some(ServerChannels::Some(v)) if v == &["chan-a".to_string()])
                );
                assert_eq!(invalid.len(), 1);
                assert_eq!(invalid[0].0, "g3");
            }
            other => panic!("expected Resolved, got {:?}", other),
        }
    }

    #[test]
    fn planning_distinguishes_nothing_to_monitor_from_pick() {
        // No config, bot in no servers -> idle.
        let mut empty: std::collections::HashMap<String, ServerChannels> = Default::default();
        assert!(matches!(
            plan_server_monitoring(&mut empty, &[]).0,
            ServerPlan::NothingToMonitor
        ));
        // No config, but the bot IS in servers -> ask the operator to pick.
        let mut empty: std::collections::HashMap<String, ServerChannels> = Default::default();
        assert!(matches!(
            plan_server_monitoring(&mut empty, &["g1".to_string()]).0,
            ServerPlan::Pick
        ));
        // A wildcard expanding to nothing must idle, NOT silently monitor zero.
        let mut wildcard = parse_servers(&parse_servers_value(&serde_json::json!({ "*": ["*"] })));
        let (plan, notes) = plan_server_monitoring(&mut wildcard, &[]);
        assert!(matches!(plan, ServerPlan::NothingToMonitor));
        assert!(notes.iter().any(|n| n.contains("0 servers")), "got {:?}", notes);
    }

    #[test]
    fn a_wildcard_is_never_left_in_the_plan_as_a_guild_id() {
        // A '*' key reaching the receive filter's `servers.get(real_guild_id)` is
        // the original bug: it matches nothing. No plan may ever contain it.
        let guilds = vec!["g1".to_string()];
        for cfg in [
            serde_json::json!({ "*": ["*"] }),
            serde_json::json!({ "*": ["chan"] }),
            serde_json::json!(["*"]),
        ] {
            let mut servers = parse_servers(&parse_servers_value(&cfg));
            plan_server_monitoring(&mut servers, &guilds);
            assert!(
                !servers.contains_key("*"),
                "config {:?} left a '*' guild key behind",
                cfg
            );
        }
    }

    #[test]
    fn guild_member_count_parses_approximate_member_count() {
        let body = serde_json::json!({
            "id": "111222",
            "name": "test guild",
            "approximate_member_count": 1234,
            "approximate_presence_count": 300
        });
        assert_eq!(guild_member_count_from_body(&body), Some(1234));
        assert_eq!(guild_member_count_from_body(&serde_json::json!({})), None);
        assert_eq!(
            guild_member_count_from_body(&serde_json::json!({"approximate_member_count": "nope"})),
            None,
            "a non-integer member count is not parsed"
        );
    }
}
