# Tailscale Web Access

Lantor exposes a browser-accessible web UI from the same desktop process so you
can open it locally or proxy it to another device. It listens only on
`127.0.0.1:8787` by default.

```bash
npm run build
npm run tauri:dev
```

Loopback requests from the same Mac can use:

```text
http://127.0.0.1:8787/
```

## Tailscale Serve

Install Tailscale on the Mac and the other device, sign both into the same
tailnet, then proxy Lantor's loopback listener:

```bash
tailscale serve --bg http://127.0.0.1:8787
tailscale serve status
```

Open the HTTPS URL reported by Tailscale from the other device:

```text
https://<mac-name>.<tailnet-name>.ts.net/
```

Tailscale Serve keeps the endpoint inside the tailnet; Lantor does not need to
listen on the Mac's LAN or Tailscale IP. Use tailnet grants or ACLs when access
should be narrower than the entire tailnet.

## Cloudflare Tunnel

A Cloudflare Tunnel running on the same Mac can also use
`http://127.0.0.1:8787` as its origin. Protect the hostname with Cloudflare
Access: a Tunnel without Access would expose Lantor's unauthenticated API to
the public internet.

## Bind overrides

To turn the web server off, set `LANTOR_WEB_BIND=off` (also accepts `none`,
`disabled`, `false`, or `0`). You can explicitly set a different address, but
non-loopback binds should only be used on a trusted network.

The web UI does not perform its own token check. Only expose Lantor on a
trusted private path such as loopback, Tailscale Serve, or a Cloudflare Tunnel
protected by Cloudflare Access.

The web UI uses HTTP endpoints under `/api/` for the subset of Tauri commands
the chat surface needs, including:

- bootstrap and runtime health checks
- sending messages, creating/updating/deleting channels and agents
- managing channel agent membership and saved messages
- inbox dismissal and read state, channel read state
- reminders (completing) and tasks (status, title, claim)
- cancelling and retrying agent work
- installing and uninstalling the supervisor LaunchAgent
- opening agent DMs
- reading artifacts and attachment preview
- agent workspace listing and file preview
- owner profile updates

Live refresh is delivered over an SSE stream at `/api/events`. Desktop Tauri
still uses native IPC for the same operations.

## Supervisor LaunchAgent

The Runtime panel can install a user LaunchAgent at:

```text
~/Library/LaunchAgents/local.lantor.supervisor.plist
```

That lets macOS keep the `--supervisor` process alive via `launchctl`.
Uninstall removes the plist and unloads the service.
