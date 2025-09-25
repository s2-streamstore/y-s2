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
    <a href="./LICENSE"><img src="https://img.shields.io/github/license/s2-streamstore/resumable-stream" /></a>
  </p>
</div>

## Overview

Y-S2 is a Cloudflare Worker that provides real-time collaborative document editing using Yjs with S2.dev as the distribution channel and an R2 bucket as the storage provider. It provides scalable WebSocket-based document synchronization where document updates are made durable on an S2 stream and distributed to connected clients and reactively persisted to the R2 bucket. 

## Getting Started

### Prerequisites

- Cloudflare account with Workers enabled.
- S2.dev account, a basin with `Create stream on append/read` enabled, and a scoped access token to the basin you want to use.
- R2 bucket for snapshot storage.

### Environment Variables

```bash
S2_ACCESS_TOKEN=your_s2_access_token
S2_BASIN=your_s2_basin_name
R2_BUCKET=your_r2_bucket_name
LOG_MODE=CONSOLE|S2_SINGLE|S2_SHARED  # Optional
SNAPSHOT_BACKLOG_SIZE=100             # Optional
```

### Deployment

```bash
# Install dependencies
npm install

# Deploy to Cloudflare Workers
npm run deploy

# Or run locally for development
npm run dev
```

### Client Integration

Connect to the deployed worker using the Yjs WebSocket provider:

```javascript
import * as Y from 'yjs'
import { WebsocketProvider } from 'y-websocket'

const doc = new Y.Doc()
const provider = new WebsocketProvider('wss://your-worker.your-subdomain.workers.dev', 'room-name', doc, {
  params: { authToken: 'your-auth-token' }
})

```

### Credits

Portions of this project are derived from [y-redis](https://github.com/yjs/y-redis), licensed under the GNU Affero General Public License v3.0.
