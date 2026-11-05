FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
COPY bin ./bin
COPY ui ./ui
# node:sqlite (archiver) is stable enough for us but still flagged experimental in 22.
ENV PORT=8090 NODE_ENV=production NODE_OPTIONS=--disable-warning=ExperimentalWarning
EXPOSE 8090
RUN mkdir -p /data && chown node:node /data
USER node
CMD ["node", "bin/xrtel-gateway.js"]
