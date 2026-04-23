FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /build
COPY target/ai-agent-1.0.0.jar app.jar

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /build/app.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
