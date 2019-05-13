FROM openjdk:12
WORKDIR /service

COPY . ./

RUN ./gradlew clean test --info
RUN ./gradlew build

CMD [ "./gradlew", "run", "--no-daemon"]
