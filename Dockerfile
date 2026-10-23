FROM node:20-alpine
RUN apk add --no-cache ffmpeg tzdata ttf-dejavu
WORKDIR /app
RUN npm install --omit=dev ws@8.18.3
COPY bridge.js .
CMD ["node", "bridge.js"]
