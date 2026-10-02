FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .
USER node
EXPOSE 8001

# API by default; the worker service overrides this with: node worker.js
CMD ["node", "index.js"]
