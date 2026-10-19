# FxSocket MCP

[![Docker Hub](https://img.shields.io/docker/v/fxsocket/mcp?sort=semver&label=docker%20hub)](https://hub.docker.com/r/fxsocket/mcp)
[![CI](https://github.com/fxsocket-com/FxSocket-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/fxsocket-com/FxSocket-MCP/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Lets Claude, Cursor, VS Code and other MCP clients read your MetaTrader 4
and 5 accounts on [FxSocket](https://fxsocket.com): balances, open
positions, closed trades with win rate and P/L, quotes, symbol
specifications and price history. **It is read-only.** It cannot place,
modify or close orders.

This image runs FxSocket's MCP server as a local **stdio** server, for
clients that start their servers themselves (Claude Desktop, Docker's MCP
Toolkit, or anything configured with a `command`). It relays each message
to `https://mcp.fxsocket.com/mcp` with your API key. The tools run on
FxSocket, next to the terminals your accounts are connected to, so the
image stays tiny and never needs updating when tools change.

If your client takes a remote server URL plus a header, you can skip the
image: point it at `https://mcp.fxsocket.com/mcp` with your key in an
`X-API-Key` header.

## 1. Get an API key

Create a **read-only key** (`fxs_ro_...`) at
[app.fxsocket.com/dashboard/api-key](https://app.fxsocket.com/dashboard/api-key).
A named key scoped to some of your accounts limits the agent to exactly
those. A full `fxs_live_...` key works too, but an agent has no use for one.

## 2. Add it to your client

**Claude Desktop** (Settings, Developer, Edit Config, then restart):

```json
{
  "mcpServers": {
    "fxsocket": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-e", "FXSOCKET_API_KEY", "fxsocket/mcp"],
      "env": { "FXSOCKET_API_KEY": "fxs_ro_..." }
    }
  }
}
```

**Claude Code:**

```bash
claude mcp add fxsocket -e FXSOCKET_API_KEY=fxs_ro_... -- docker run -i --rm -e FXSOCKET_API_KEY fxsocket/mcp
```

**Cursor** (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "fxsocket": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-e", "FXSOCKET_API_KEY", "fxsocket/mcp"],
      "env": { "FXSOCKET_API_KEY": "fxs_ro_..." }
    }
  }
}
```

**VS Code** (`.vscode/mcp.json`, prompts for the key once):

```json
{
  "inputs": [
    { "type": "promptString", "id": "fxsocket-key", "description": "FxSocket API key", "password": true }
  ],
  "servers": {
    "fxsocket": {
      "type": "stdio",
      "command": "docker",
      "args": ["run", "-i", "--rm", "-e", "FXSOCKET_API_KEY", "fxsocket/mcp"],
      "env": { "FXSOCKET_API_KEY": "${input:fxsocket-key}" }
    }
  }
}
```

`-e FXSOCKET_API_KEY` without a value passes the variable through from the
client's `env`, so the key never appears in the process list.

**Without Docker:** the relay is one file with no dependencies. Download
[`fxsocket_mcp.py`](fxsocket_mcp.py) and use `python3` (3.10 or newer) as
the command with the file's path as the argument, keeping the same `env`.

## Tools

| Tool | What it answers |
|---|---|
| `list_accounts` | Every account the key can see, optionally with balance, equity and totals per currency. Call first. |
| `get_account` | One account in full: summary, broker info and terminal status. |
| `get_open_positions` | Open positions and pending orders across accounts, with floating P/L and swap. |
| `get_trade_history` | Closed trades for a period, with wins, losses, win rate, net P/L and a per-symbol breakdown. |
| `search_symbols` | Find the broker's exact symbol name (`EURUSD`, `EURUSD.sd`, `EURUSDm`...). |
| `get_quotes` | Live bid/ask and spread for up to 20 symbols. |
| `get_symbol_info` | Contract specification: digits, contract size, volume limits, swaps, sessions. |
| `get_price_history` | OHLC candles for a symbol and timeframe. |

Amounts are in each account's deposit currency and are never summed across
currencies. Times are broker server time.

## Configuration

| Variable | Default | |
|---|---|---|
| `FXSOCKET_API_KEY` | (required) | Your FxSocket API key. |
| `FXSOCKET_MCP_URL` | `https://mcp.fxsocket.com/mcp` | The server to relay to. Plain `http` is only allowed to localhost. |
| `FXSOCKET_MCP_TIMEOUT` | `120` | Seconds to wait for one reply. |
| `FXSOCKET_MCP_LOG` | `info` | `error`, `warning`, `info` or `debug`. Logs go to stderr. |

FxSocket allows 60 MCP requests a minute per user.

## How it works

The client writes newline-delimited JSON-RPC to the container's stdin. The
relay POSTs each message to the MCP endpoint over a kept-alive HTTPS
connection, adds the API key and the headers the protocol requires, and
writes the replies to stdout. It handles both protocol eras:

- **2026-07-28:** the protocol version travels in each request's `_meta`,
  and the relay mirrors it into the `MCP-Protocol-Version`, `Mcp-Method`
  and `Mcp-Name` headers (plus any `x-mcp-header` tool parameters).
- **2025-03-26 to 2025-11-25:** the client opens with `initialize`, and the
  relay sends the negotiated version as `MCP-Protocol-Version` from then on.

If FxSocket rejects the key, the relay still answers `initialize` and
`tools/list` from the server's public card
(`https://mcp.fxsocket.com/.well-known/mcp/server-card.json`), so
directories and inspectors that start it with a placeholder key can see
the tools. Every tool call still needs a valid key and otherwise returns
the rejection.

Requests run concurrently. A `notifications/cancelled` from the client
closes that request's connection, which is how Streamable HTTP cancels.
HTTP failures (bad key, rate limit, network) come back as JSON-RPC errors
with a sentence saying what to fix, rather than a hung client.

## Development

```bash
python3 -m unittest discover -s tests
docker build -t fxsocket/mcp:dev .
```

The tests start the relay as a subprocess against a local stub server, so
they cover the real stdio and HTTP paths.

To release, bump `__version__` in `fxsocket_mcp.py`, then push a matching
tag (`git tag v1.0.1 && git push --tags`). The `publish` workflow builds
`linux/amd64` and `linux/arm64` and pushes `fxsocket/mcp:<version>`,
`:<major>.<minor>` and `:latest`, and the `dockerhub-overview` workflow
copies this README to the Docker Hub page.

## License

[MIT](LICENSE)
