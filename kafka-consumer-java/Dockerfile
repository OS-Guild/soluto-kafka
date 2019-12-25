FROM openjdk:13-alpine
WORKDIR /service

COPY . ./

RUN ./gradlew clean test --info
RUN ./gradlew build

CMD ["gradlew", "run", "--no-daemon"]