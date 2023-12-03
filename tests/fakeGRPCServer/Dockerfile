FROM node:10-alpine

WORKDIR /app

COPY soluto-kafka-grpc-target ./soluto-kafka-grpc-target
COPY tests/fakeGRPCServer/package.json .

WORKDIR /app/soluto-kafka-grpc-target

RUN yarn

WORKDIR /app

RUN yarn

COPY tests/fakeGRPCServer .

RUN yarn build

EXPOSE 3003
EXPOSE 6004
CMD [ "node", "dist/index.js" ]