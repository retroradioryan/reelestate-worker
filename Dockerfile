FROM node:22-bookworm

# Install ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package files first
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm install

# Copy rest of app
COPY . .

EXPOSE 10000

CMD ["npm", "start"]
