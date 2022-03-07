FROM openjdk:13-alpine AS base
WORKDIR /service

FROM base AS build
COPY . ./

RUN ./gradlew clean --info --no-daemon
RUN ./gradlew build --info --no-daemon

FROM base AS release
WORKDIR /service

COPY --from=build /service/build/libs/*-all.jar /service/application.jar

ENTRYPOINT ["java", "-jar","/service/application.jar"]