FROM openjdk:11 as base
WORKDIR /service

COPY . ./
RUN ./gradlew build

CMD [ "./gradlew", "run", "--no-daemon"]
