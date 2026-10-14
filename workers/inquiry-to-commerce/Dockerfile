FROM node:26-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
COPY packages/events/package.json packages/events/package.json
COPY workers/inquiry-to-commerce/package.json workers/inquiry-to-commerce/package.json
RUN npm ci
COPY packages/events/src packages/events/src
COPY workers/inquiry-to-commerce/src workers/inquiry-to-commerce/src
COPY workers/inquiry-to-commerce/tsconfig.json workers/inquiry-to-commerce/tsconfig.json
RUN npm run build --workspace @fionas/inquiry-to-commerce

FROM node:26-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/events/package.json packages/events/package.json
COPY workers/inquiry-to-commerce/package.json workers/inquiry-to-commerce/package.json
RUN npm ci --omit=dev --workspace @fionas/inquiry-to-commerce && npm cache clean --force
COPY --from=build /app/workers/inquiry-to-commerce/dist ./workers/inquiry-to-commerce/dist
COPY scripts/nats-setup.mjs ./scripts/nats-setup.mjs
COPY scripts/nats-tail.mjs ./scripts/nats-tail.mjs
USER node
CMD ["node", "workers/inquiry-to-commerce/dist/workers/inquiry-to-commerce/src/main.js"]
