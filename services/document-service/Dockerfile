FROM node:18

WORKDIR /app

COPY package.json .
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*
RUN npm install

COPY . .

CMD ["node", "app.js"]