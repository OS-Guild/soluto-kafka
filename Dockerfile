FROM openjdk:13 as base
WORKDIR /service

COPY . ./
RUN ./gradlew build

CMD [ "./gradlew", "run", "--no-daemon"]
