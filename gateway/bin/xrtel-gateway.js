#!/usr/bin/env node
'use strict';
// Configuration from the environment, so the same image runs anywhere.
const fs = require('fs');
const { createGateway } = require('../src/gateway');
const env = process.env;
// TLS_KEY + TLS_CERT: serve wss:// directly, e.g. on a LAN with the same
// self-signed certificate the headset already accepted for the player.
const tls = env.TLS_KEY && env.TLS_CERT
  ? { key: fs.readFileSync(env.TLS_KEY), cert: fs.readFileSync(env.TLS_CERT) } : null;
const gw = createGateway({
  redisUrl:     env.REDIS_URL || 'redis://127.0.0.1:6379',
  namespace:    env.XRTEL_NAMESPACE || '',
  port:         env.PORT ? +env.PORT : 8090,
  basePath:     env.BASE_PATH || '',
  ingestToken:  env.INGEST_TOKEN || null,
  consumeToken: env.CONSUME_TOKEN || null,
  maxlen:       env.STREAM_MAXLEN ? +env.STREAM_MAXLEN : 20000,
  archiverUrl:  env.ARCHIVER_URL || null,
  tls,
});
const stop = () => gw.close().then(() => process.exit(0));
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
