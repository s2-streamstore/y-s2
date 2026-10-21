<div align="center">
  <p>
    <!-- Light mode logo -->
    <a href="https://s2.dev#gh-light-mode-only">
      <img src="./assets/s2-black.png" height="60">
    </a>
    <!-- Dark mode logo -->
    <a href="https://s2.dev#gh-dark-mode-only">
      <img src="./assets/s2-white.png" height="60">
    </a>
  </p>

  <h1>Yjs - S2 Cloudflare Worker</h1>

  <p>
    <a href="https://s2.dev/demos/y-s2">
      <img src="https://img.shields.io/badge/_Try_Demo-Live-brightgreen?style=for-the-badge" alt="Try Demo">
    </a>
  </p>

  <p>
    <!-- Discord (chat) -->
    <a href="https://discord.gg/vTCs7kMkAf"><img src="https://img.shields.io/discord/1209937852528599092?logo=discord" /></a>
    <!-- LICENSE -->
    <a href="./LICENSE"><img src="https://img.shields.io/github/license/s2-streamstore/y-s2" /></a>
  </p>
</div>

## Overview

Y-S2 is a Cloudflare Worker that provides real-time collaborative document editing using Yjs with S2.dev as the distribution channel and an R2 bucket as the storage provider. It provides scalable WebSocket-based document synchronization where document updates are made durable on an S2 stream and distributed to connected clients and reactively persisted to the R2 bucket. 

## Getting Started

### Prerequisites

- Cloudflare account on the Workers Paid plan (each WebSocket connection is one Worker invocation, and the free plan allows only 50 subrequests per invocation).
- S2.dev account, a basin with `Create stream on append/read` enabled, and a scoped access token to the basin you want to use.
- R2 bucket for snapshot storage.

### Configuration

Set the S2 credentials as Worker secrets, and point the `R2_BUCKET` binding in `wrangler.jsonc` at your bucket (`bucket_name`):

```bash
npx wrangler secret put S2_ACCESS_TOKEN
npx wrangler secret put S2_BASIN
```

Optional variables (under `vars` in `wrangler.jsonc`, or in `.dev.vars` locally):

| Variable                 | Default   | Description                                                                 |
| ------------------------ | --------- | --------------------------------------------------------------------------- |
| `LOG_MODE`               | `CONSOLE` | `CONSOLE`, `S2_SINGLE` (one log stream per connection) or `S2_SHARED`        |
| `SNAPSHOT_BACKLOG_SIZE`  | `400`     | Records since the last snapshot that trigger a new one                      |
| `SNAPSHOT_BACKLOG_BYTES` | `8388608` | Bytes since the last snapshot that trigger a new one                        |
| `BACKLOG_BUFFER_AGE`     | `60000`   | Milliseconds after the oldest unsnapshotted record before a snapshot        |
| `LEASE_DURATION`         | `30`      | Seconds a connection holds the snapshot lease                               |
| `S2_BATCH_SIZE`          | `8`       | Maximum records per append batch                                            |
| `S2_LINGER_TIME`         | `50`      | Milliseconds to wait for more records before sending an append batch        |

Set the basin's default stream retention to infinite: snapshots are only taken while a room is active, so updates that expire from the stream before a snapshot covers them are lost.

### Deployment

Requires Node.js 22 or newer.

```bash
# Install dependencies
npm install

# Deploy to Cloudflare Workers
npm run deploy

# Or run locally for development
npm run dev
```

#### Continuous deployment

Every push to `main` is checked (lint, format, typecheck, tests, build) and deployed to Cloudflare Workers by GitHub Actions. The workflow expects these repository secrets:

| Secret                  | Description                                         |
| ----------------------- | --------------------------------------------------- |
| `CLOUDFLARE_API_TOKEN`  | API token with the "Edit Cloudflare Workers" scope  |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare account ID                               |
| `R2_BUCKET_NAME`        | R2 bucket used for snapshots                        |
| `S2_ACCESS_TOKEN`       | S2 access token scoped to the basin                 |
| `S2_BASIN`              | S2 basin name                                       |

### Client Integration

Connect with the Yjs WebSocket provider. The server URL ends in `/ws`, and the room is passed both as the room name and as the `room` parameter:

```javascript
import * as Y from 'yjs'
import { WebsocketProvider } from 'y-websocket'

const room = 'room-name'
const doc = new Y.Doc()
const provider = new WebsocketProvider('wss://your-worker.your-subdomain.workers.dev/ws', room, doc, {
  params: { room, yauth: 'your-auth-token' },
})
```

Clients are not authenticated yet: any non-empty `yauth` is accepted (see #4).

### Credits

Portions of this project are derived from [y-redis](https://github.com/yjs/y-redis), licensed under the GNU Affero General Public License v3.0.
