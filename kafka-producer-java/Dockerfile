FROM openjdk:13-alpine
WORKDIR /service

COPY . ./
RUN ./gradlew build

CMD [ "./gradlew", "run", "$JVM_OPTS"]