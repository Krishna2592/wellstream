FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
COPY target/producer-1.0.0-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]