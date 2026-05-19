FROM node:20

WORKDIR /app

COPY package*.json ./

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*
RUN npm install

COPY . .

EXPOSE 3000

CMD ["node", "app.js"]