#!/usr/bin/env node
'use strict';
// Archiver: every Redis stream device:*:* into SQLite, plus the sessions/export API
// the gateway proxies to. Configuration from the environment, like the gateway.
const { createArchiver } = require('../src/archiver');
const env = process.env;
const ar = createArchiver({
  redisUrl:  env.REDIS_URL || 'redis://127.0.0.1:6379',
  namespace: env.XRTEL_NAMESPACE || '',
  dbPath:    env.ARCHIVE_DB || 'xrtel.sqlite',
  port:      env.PORT ? +env.PORT : 8091,
  consumer:  env.ARCHIVER_NAME || 'archiver-1',
});
const stop = () => ar.close().then(() => process.exit(0));
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
