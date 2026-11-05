#!/usr/bin/env node
'use strict';
// Configuration from the environment, so the same image runs anywhere.
const { createGateway } = require('../src/gateway');
const env = process.env;
const gw = createGateway({
  redisUrl:     env.REDIS_URL || 'redis://127.0.0.1:6379',
  namespace:    env.XRTEL_NAMESPACE || '',
  port:         env.PORT ? +env.PORT : 8090,
  basePath:     env.BASE_PATH || '',
  ingestToken:  env.INGEST_TOKEN || null,
  consumeToken: env.CONSUME_TOKEN || null,
  maxlen:       env.STREAM_MAXLEN ? +env.STREAM_MAXLEN : 20000,
});
const stop = () => gw.close().then(() => process.exit(0));
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
