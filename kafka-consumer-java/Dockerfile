FROM openjdk:12
WORKDIR /service

COPY . ./
RUN ./gradlew build

CMD [ "./gradlew", "run", "--no-daemon"]
