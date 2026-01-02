FROM maven:3.9-eclipse-temurin-23 AS build

WORKDIR /app
COPY pom.xml .

RUN mvn dependency:go-offline -B

COPY src ./src

RUN mvn clean package -DskipTests

FROM eclipse-temurin:23-jre

WORKDIR /app

COPY --from=build /app/target/InMemoryDB-1.0-SNAPSHOT-jar-with-dependencies.jar /app/kvstore.jar

EXPOSE 9000-9010 5005

ENTRYPOINT ["java", "-cp", "/app/kvstore.jar", "com.db.memory.KVNode"]