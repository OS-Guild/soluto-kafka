FROM openjdk:11.0.2-slim as base
WORKDIR /service

COPY . ./
RUN ./gradlew build

CMD [ "./gradlew", "run", "--no-daemon"]
