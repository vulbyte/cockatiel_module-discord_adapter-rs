use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::{proto::container::Payload, proto::*, CockatielClient, PromptKind};

// GUILD_MESSAGES (1<<9) + MESSAGE_CONTENT (1<<15). MESSAGE_CONTENT is a
// privileged intent — enable it in the Discord Developer Portal for the bot.
const DISCORD_INTENTS: u64 = (1 << 9) | (1 << 15);
const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";
const REST_API: &str = "https://discord.com/api/v10";

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

/// Channel policy for one server.
#[derive(Debug, Clone)]
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

/// All explicitly-listed channel IDs across every server (used for
/// SendToPlatforms; * servers contribute none since we can't enumerate).
fn all_send_channels(servers: &std::collections::HashMap<String, ServerChannels>) -> Vec<String> {
    let mut out = Vec::new();
    for policy in servers.values() {
        if let ServerChannels::Some(chs) = policy {
            out.extend(chs.iter().cloned());
        }
    }
    out.sort();
    out.dedup();
    out
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct DiscordAdapterConfig {
    bot_token: Option<String>,
    #[serde(default)]
    servers: std::collections::HashMap<String, Vec<String>>,
    #[serde(default)]
    embed_sends: bool,
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
        .and_then(|s| serde_json::from_value(s.clone()).ok())
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

    Some(DiscordAdapterConfig { bot_token: Some(bot_token), servers, embed_sends })
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
    json_val["module_specific"] = json!({ "servers": serialize_servers(servers) });
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

/// Parse a moderator command (!ban / !timeout) from a chat message.
/// Returns (query_id, payload_json). The actor (message author) is included.
fn parse_mod_command(message: &str, author: &str) -> Option<(String, serde_json::Value)> {
    let trimmed = message.trim();
    let lower = trimmed.to_lowercase();

    if lower.starts_with("!ban") {
        let args = trimmed[5..].trim();
        let (target, rest) = match args.split_once(char::is_whitespace) {
            Some((t, r)) => (t, r),
            None => (args, ""),
        };
        let target = clean_target(target);
        if target.is_empty() {
            return None;
        }
        // Discord bans are permanent — a timed ban (`!ban @user -d 300`) is a
        // timeout instead (matches how the engine's mod_timeout works).
        let mut duration_secs: Option<i64> = None;
        let mut reason = rest.trim().to_string();
        if let Some(dpos) = reason.find("-d") {
            let after = reason[dpos + 2..].trim();
            let (num, _) = after.split_once(char::is_whitespace).unwrap_or((after, ""));
            if let Ok(secs) = num.parse::<i64>() {
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
        return Some((
            "mod_ban".to_string(),
            serde_json::json!({
                "platform": "discord",
                "handle": target,
                "reason": reason,
                "actor": { "platform": "discord", "handle": author },
            }),
        ));
    }

    if lower.starts_with("!timeout") {
        let args = trimmed[9..].trim();
        let mut parts = args.split_whitespace();
        let target = parts.next().unwrap_or("").to_string();
        let target = clean_target(&target);
        if target.is_empty() {
            return None;
        }
        let mut duration_secs = 300i64;
        let mut reason = String::new();
        if let Some(d) = parts.next() {
            if let Ok(secs) = d.parse::<i64>() {
                duration_secs = secs;
            } else {
                reason = d.to_string();
            }
        }
        let rest: Vec<&str> = parts.collect();
        if !rest.is_empty() {
            if !reason.is_empty() {
                reason = format!("{} {}", reason, rest.join(" "));
            } else {
                reason = rest.join(" ");
            }
        }
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

    None
}

// ── Discord REST helpers ──────────────────────────────────────────────

async fn send_discord_message(
    client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    msg: &str,
    embed: bool,
) -> Result<(), String> {
    // `embed_sends: true` posts a discordjs-style embed instead of a plain
    // message (ROADMAP: "receive a Send and post as an embed").
    let body = if embed {
        json!({ "embeds": [{ "description": msg }] })
    } else {
        json!({ "content": msg })
    };
    let resp = client
        .post(format!("{}/channels/{}/messages", REST_API, channel_id))
        .bearer_auth(token)
        .json(&body)
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

// ── Discord gateway ───────────────────────────────────────────────────

/// Connect to the Discord gateway, identify, heartbeat, and forward
/// MESSAGE_CREATE events to the engine as preprocessed messages. When the bot
/// token is rejected (op 9 / close 4004), it asks the operator for a fresh
/// token via the prompt subwindow instead of retrying the bad token forever.
async fn run_discord_gateway(
    token: String,
    servers: std::collections::HashMap<String, ServerChannels>,
    member_cache: Arc<Mutex<HashMap<String, String>>>,
    engine_write: Arc<Mutex<WsWriteHalf>>,
    send_token: Arc<Mutex<String>>,
    send_channels: Arc<Mutex<Vec<String>>>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
) {
    let mut servers = servers;
    let mut token = token;
    let mut auth_failures = 0u32;
    loop {
        let (mut ws, _) = match tokio_tungstenite::connect_async(GATEWAY_URL).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Failed to connect to Discord gateway ({}). Likely causes: invalid bot token, the Message Content intent is not enabled in the Developer Portal, or the bot is not in the guild. Retrying in 5s...",
                    e
                );
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
                                // 4004/4005 = authentication failed — the saved
                                // token is bad. Count it so the operator is
                                // prompted for a fresh token instead of looping.
                                let code: u16 = f.code.into();
                                if code == 4004 || code == 4005 {
                                    auth_failures += 1;
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
                    // op 9 = Invalid Session (bad token / session expired).
                    if op == 9 {
                        warn!("Discord gateway reported an invalid session (op 9) — the bot token may be wrong.");
                        auth_failures += 1;
                        break 'conn;
                    }
                    if op == 0 {
                        let t = payload.get("t").and_then(|v| v.as_str()).unwrap_or("");
                        let d = payload.get("d");
                        match t {
                            "READY" => {
                                info!("Discord gateway ready (bot: {}).", d.and_then(|x| x.get("user")).and_then(|u| u.get("username")).and_then(|u| u.as_str()).unwrap_or("?"));

                                // READY carries `d.guilds` — every server the bot
                                // belongs to. We monitor every configured server.
                                let bot_guilds: Vec<String> = d
                                    .and_then(|x| x.get("guilds"))
                                    .and_then(|g| g.as_array())
                                    .map(|arr| {
                                        arr.iter()
                                            .filter_map(|g| g.get("id").and_then(|i| i.as_str()))
                                            .map(|s| s.to_string())
                                            .collect()
                                    })
                                    .unwrap_or_default();

                                // Verify each configured server; drop invalid ones.
                                if !servers.is_empty() {
                                    let mut valid: std::collections::HashMap<String, ServerChannels> = Default::default();
                                    for (guild, policy) in &servers {
                                        if bot_guilds.contains(guild) {
                                            valid.insert(guild.clone(), policy.clone());
                                        } else {
                                            error!(
                                                "Discord bot is NOT in configured server {} — it can only access: [{}]. That server will be skipped.",
                                                guild,
                                                bot_guilds.join(", ")
                                            );
                                        }
                                    }
                                    if !valid.is_empty() {
                                        servers = valid;
                                        let _ = *send_channels.lock().await = all_send_channels(&servers);
                                        let desc: Vec<String> = servers
                                            .iter()
                                            .map(|(g, p)| match p {
                                                ServerChannels::All => format!("{}=[*]", g),
                                                ServerChannels::Some(chs) => format!("{}={}", g, chs.join(",")),
                                            })
                                            .collect();
                                        info!("Monitoring servers: {}", desc.join(" · "));
                                    } else {
                                        // Every configured server is invalid — drop
                                        // them and prompt for a fresh selection.
                                        servers = Default::default();
                                    }
                                }

                                // No servers configured (first launch or all invalid):
                                // ask the operator which server(s) + channels to monitor.
                                if servers.is_empty() {
                                    let mut accessible_list = String::new();
                                    for (i, g) in bot_guilds.iter().enumerate() {
                                        accessible_list.push_str(&format!("  {}: {}\n", i + 1, g));
                                    }
                                    let accessible = if bot_guilds.is_empty() {
                                        "  (none — the bot has no server access at all)".to_string()
                                    } else {
                                        accessible_list.trim_end().to_string()
                                    };

                                    // 1) Pick a server.
                                    if let Some(choice) = prompt_for_input(
                                        &engine_write,
                                        prompt_rx,
                                        auth_token,
                                        module_name,
                                        instance_uuid,
                                        "Choose the Discord Server",
                                        &format!(
                                            "Your bot can currently only access these servers:\n{}\n\n\
                                             Enter the NUMBER of a server above (e.g. 1), or paste a Server (Guild) ID directly.\n\n\
                                             To find a Server ID: Settings > Advanced > Developer Mode >\n\
                                             right-click the server name > Copy Server ID.\n\n\
                                             Leave empty (or press Cancel) to keep retrying.",
                                            accessible
                                        ),
                                        "Server number or Guild ID",
                                        PromptKind::String,
                                        300,
                                    )
                                    .await
                                    {
                                        let trimmed = choice.trim().to_string();
                                        let mut new_guild = String::new();
                                        if trimmed.is_empty() || trimmed == "0" {
                                            warn!("No server selected — keeping current setting and retrying.");
                                        } else if let Ok(idx) = trimmed.parse::<usize>() {
                                            if idx >= 1 && idx <= bot_guilds.len() {
                                                new_guild = bot_guilds[idx - 1].clone();
                                                info!("Discord Server ID set to {} (selection {}).", new_guild, idx);
                                            } else {
                                                warn!("Server number {} is out of range (1..={}). Keeping current setting.", idx, bot_guilds.len());
                                            }
                                        } else {
                                            new_guild = trimmed.clone();
                                            info!("Discord Server ID set to {} (pasted).", new_guild);
                                        }

                                        // 2) Channels for that server (empty = all).
                                        if !new_guild.is_empty() {
                                            let mut chs: Vec<String> = Vec::new();
                                            if let Some(ch_choice) = prompt_for_input(
                                                &engine_write,
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
                                                300,
                                            )
                                            .await
                                            {
                                                let tc = ch_choice.trim();
                                                if !tc.is_empty() {
                                                    chs = tc.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
                                                }
                                            }
                                            let policy = if chs.is_empty() {
                                                ServerChannels::All
                                            } else {
                                                ServerChannels::Some(chs)
                                            };
                                            servers.insert(new_guild, policy);
                                        }
                                    }

                                    if !servers.is_empty() {
                                        save_adapter_config(&token, &servers);
                                        let _ = *send_channels.lock().await = all_send_channels(&servers);
                                        info!("Discord configuration updated: {} server(s) (reconnecting...)", servers.len());
                                        break 'conn;
                                    } else {
                                        // No server chosen — back off so we don't
                                        // re-prompt every few seconds in a loop.
                                        warn!("No Discord server configured yet — retrying in 60s.");
                                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                                        break 'conn;
                                    }
                                }
                            }
                            "MESSAGE_CREATE" => {
                                if let Some(d) = d {
                                    handle_message_create(
                                        d,
                                        &servers,
                                        &member_cache,
                                        &engine_write,
                                        auth_token,
                                        module_name,
                                        instance_uuid,
                                    )
                                    .await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        warn!("Discord gateway disconnected. Reconnecting in 5s...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Repeated auth failures mean the saved token is bad — ask the operator
        // for a fresh one via the prompt subwindow instead of looping forever.
        if auth_failures >= 2 {
            auth_failures = 0;
            info!("Prompting for a new Discord bot token (previous token was rejected)...");
            if let Some(new_token) = prompt_for_input(
                &engine_write,
                prompt_rx,
                auth_token,
                module_name,
                instance_uuid,
                "Discord Bot Token Required",
                "Discord rejected the current bot token (invalid session / authentication failed).\n\n\
                 Paste a valid bot token from the Discord Developer Portal\n\
                 (discord.com/developers > your app > Bot > Reset Token).\n\n\
                 The token is masked so it never shows on screen.",
                "Bot Token",
                PromptKind::Credential,
                300,
            )
            .await
            {
                let trimmed = new_token.trim().to_string();
                if !trimmed.is_empty() {
                    token = trimmed.clone();
                    *send_token.lock().await = token.clone();
                    save_adapter_config(&token, &servers);
                    info!("Discord bot token updated.");
                }
            }
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
    member_cache: &Arc<Mutex<HashMap<String, String>>>,
    engine_write: &Arc<Mutex<WsWriteHalf>>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
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
    let author_id = d.get("author").and_then(|a| a.get("id")).and_then(|u| u.as_str()).unwrap_or("");
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

    // Cache the author id -> username so `<@id>` mention targets can resolve.
    if !author_id.is_empty() {
        member_cache.lock().await.insert(author_id.to_string(), author.to_string());
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
            user_data: None,
        }),
    };
    let container = Container {
        version: 1,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: instance_uuid.to_string(),
        payload: Some(Payload::MessagePreProcess(pre)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut write = engine_write.lock().await;
        let _ = write.send(WsMessage::Binary(buf.into())).await;
    }
}

// ── main ──────────────────────────────────────────────────────────────

/// Send a Prompt to the engine (forwarded to connected UIs) and wait for the
/// operator's response (`PromptResponse.reason`). Returns None on cancel/timeout.
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
    let container = Container {
        version: 1,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: instance_uuid.to_string(),
        payload: Some(Payload::Prompt(prompt)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_err() {
        return None;
    }
    {
        let mut write = engine_write.lock().await;
        if write.send(WsMessage::Binary(buf.into())).await.is_err() {
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
    cockatiel_client::load_env_file(".env");
    let mut bot_token = std::env::var("DISCORD_BOT_TOKEN").unwrap_or_default();
    let mut servers: std::collections::HashMap<String, ServerChannels> = Default::default();
    if let Some(saved) = load_adapter_config() {
        if let Some(t) = saved.bot_token {
            if !t.is_empty() {
                bot_token = t;
            }
        }
        servers = parse_servers(&saved.servers);
    }

    // Connect to the engine first so prompts can be surfaced to connected UIs
    // before the adapter is configured.
    let cockatiel = CockatielClient::connect("config.json").await?;
    let auth_token = cockatiel.auth_token.clone();
    let instance_uuid = cockatiel.instance_uuid7.clone();
    let module_name = cockatiel.config.module_name.clone();
    let (write, read) = cockatiel.stream.split();

    let engine_write: Arc<Mutex<WsWriteHalf>> = Arc::new(Mutex::new(write));

    // Register the mod commands with the engine command system: the engine now
    // parses `!ban` / `!timeout` and routes them back with the parsed Command.
    {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let commands = Container {
            version: 1,
            auth_token: auth_token.clone(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_uuid.clone(),
            payload: Some(Payload::CommandsPayload(Commands {
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
        use prost::Message;
        if commands.encode(&mut cbuf).is_ok() {
            let mut w = engine_write.lock().await;
            let _ = w.send(WsMessage::Binary(cbuf.into())).await;
        }
        info!("registered !ban / !timeout commands");
    }
    let member_cache: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let http = reqwest::Client::new();

    // Send state for the read task, populated once config resolves.
    let send_token: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let send_channels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let send_embed: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    // Channel carrying PromptResponses from the engine to the config loop, so
    // `prompt_for_input` can await the operator's typed answer.
    let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel::<PromptResponse>();

    // Read task: engine -> adapter (SendToPlatforms / PromptResponse / AuthVerify).
    {
        let token_state = send_token.clone();
        let channels_state = send_channels.clone();
        let embed_state = send_embed.clone();
        let http = http.clone();
        let engine_write = engine_write.clone();
        let auth_token = auth_token.clone();
        let module_name = module_name.clone();
        let instance_uuid = instance_uuid.clone();
        tokio::spawn(async move {
            let mut read = read;
            while let Some(msg) = read.next().await {
                let Ok(WsMessage::Binary(data)) = msg else { continue };
                let Ok(container) = Container::decode(data.as_ref()) else { continue };
                let Some(payload) = container.payload else { continue };
                match payload {
                    // Answer the engine's liveness probe with our auth token so
                    // a quiet period never gets us severed as unresponsive.
                    Payload::AuthVerify(_) => {
                        let reply = Container {
                            version: 1,
                            auth_token: auth_token.clone(),
                            module_name: module_name.clone(),
                            module_instance_uuid7: instance_uuid.clone(),
                            payload: Some(Payload::AuthVerify(AuthVerify {
                                cur_auth: auth_token.clone(),
                            })),
                        };
                        let mut buf = Vec::new();
                        if reply.encode(&mut buf).is_ok() {
                            let mut w = engine_write.lock().await;
                            let _ = w.send(WsMessage::Binary(buf.into())).await;
                        }
                    }
                    Payload::SendToPlatforms(send) => {
                        let token = token_state.lock().await.clone();
                        let channels = channels_state.lock().await.clone();
                        let embed = *embed_state.lock().await;
                        // Send to every monitored channel.
                        if channels.is_empty() {
                            warn!("SendToPlatforms received but no channels configured to send to.");
                            continue;
                        }
                        for ch in &channels {
                            match send_discord_message(&http, &token, ch, &send.msg, embed).await {
                                Ok(()) => info!("Sent to Discord channel {}: {}", ch, send.msg),
                                Err(e) => error!("SendToPlatforms failed on {}: {}", ch, e),
                            }
                        }
                    }
                    Payload::PromptResponse(resp) => {
                        // Forward operator answers to the awaiting prompt.
                        let _ = prompt_tx.send(resp);
                    }
                    // Routed chat command: the engine parsed `!ban` / `!timeout`
                    // and delivered it here with the parsed Command attached.
                    Payload::MessagePreProcess(pre) => {
                        let Some(chat) = pre.raw_message else { continue };
                        let Some(cmd) = chat.command else { continue };
                        if cmd.command_name != "ban" && cmd.command_name != "timeout" {
                            continue;
                        }
                        let author = chat
                            .user_data
                            .as_ref()
                            .map(|u| u.username.clone())
                            .unwrap_or_default();
                        if let Some((qid, payload)) = parse_mod_command(&chat.raw_message, &author) {
                            let query = Container {
                                version: 1,
                                auth_token: auth_token.clone(),
                                module_name: module_name.clone(),
                                module_instance_uuid7: instance_uuid.clone(),
                                payload: Some(Payload::DatabaseQuery(DatabaseQuery {
                                    query_id: qid,
                                    sql: payload.to_string(),
                                    params: vec![],
                                })),
                            };
                            let mut qbuf = Vec::new();
                            if query.encode(&mut qbuf).is_ok() {
                                let mut w = engine_write.lock().await;
                                let _ = w.send(WsMessage::Binary(qbuf.into())).await;
                            }
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    // ── Config phase ───────────────────────────────────────────────────
    // Only the bot token is required here. The server→channel map is loaded
    // from config.json (and finalized/validated at READY time).
    if bot_token.is_empty() {
        if let Some(saved) = load_adapter_config() {
            if let Some(st) = saved.bot_token {
                if !st.is_empty() {
                    bot_token = st;
                }
            }
            servers = parse_servers(&saved.servers);
        }
    }

    if bot_token.is_empty() {
        // Prompt for the bot token (masked credential).
        if let Some(val) = prompt_for_input(
            &engine_write,
            &mut prompt_rx,
            &auth_token,
            &module_name,
            &instance_uuid,
            "Discord Bot Token Required",
            &setup_guide_text(),
            "Bot Token",
            PromptKind::Credential,
            120,
        )
        .await
        {
            bot_token = val.trim().to_string();
        }

        // Fallback: if the operator cancelled, keep polling config.json so the
        // TUI can still supply a token via file.
        while bot_token.is_empty() {
            if let Some(saved) = load_adapter_config() {
                if let Some(st) = saved.bot_token {
                    if !st.is_empty() {
                        bot_token = st;
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }

    // Publish the token to the read task (servers/channels are finalized at
    // READY and synced by the gateway).
    *send_token.lock().await = bot_token.clone();
    *send_channels.lock().await = all_send_channels(&servers);
    let embed_sends = load_adapter_config().map(|c| c.embed_sends).unwrap_or(false);
    *send_embed.lock().await = embed_sends;

    // Discord gateway task.
    info!("Connecting to Discord gateway...");
    run_discord_gateway(
        bot_token,
        servers,
        member_cache,
        engine_write,
        send_token,
        send_channels,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
    )
    .await;

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{all_send_channels, parse_servers, ServerChannels, extract_image_urls, parse_mod_command};
    use serde_json::json;

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
    fn all_send_channels_union_and_skips_all() {
        let input: std::collections::HashMap<String, Vec<String>> = serde_json::from_value(json!({
            "g1": ["ch1", "ch2"],
            "g2": ["*"],
            "g3": ["ch3"],
        })).unwrap();
        let servers = parse_servers(&input);
        let mut send = all_send_channels(&servers);
        send.sort();
        assert_eq!(send, vec!["ch1".to_string(), "ch2".to_string(), "ch3".to_string()]);
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
        let (qid, payload) = parse_mod_command("!ban @user -d 300 spamming", "mod").unwrap();
        assert_eq!(qid, "mod_timeout");
        assert_eq!(payload["duration_secs"], 300);
        assert_eq!(payload["handle"], "user");
        assert_eq!(payload["reason"], "spamming");
        assert_eq!(payload["actor"]["handle"], "mod");

        // Plain ban stays a permanent mod_ban.
        let (qid, payload) = parse_mod_command("!ban @user being awful", "mod").unwrap();
        assert_eq!(qid, "mod_ban");
        assert_eq!(payload["reason"], "being awful");
    }
}
