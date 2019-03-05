FROM openjdk:11.0.2-slim
WORKDIR /service

COPY . ./
RUN ./gradlew build

CMD [ "./gradlew", "run", "--no-daemon"]
