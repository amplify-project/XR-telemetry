FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
COPY bin ./bin
ENV PORT=8090 NODE_ENV=production
EXPOSE 8090
USER node
CMD ["node", "bin/xrtel-gateway.js"]
