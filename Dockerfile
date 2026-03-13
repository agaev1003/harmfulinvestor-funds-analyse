FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s \
  CMD node -e "fetch('http://localhost:3000/api/healthz').then(r=>{if(!r.ok)throw 1})"
CMD ["node", "server.js"]
