# Runtime image for the single-JVM module. Runs a PREBUILT jar (build with local Corretto 25):
#
#   mvn -DskipTests package
#   docker build -t reactive-local-caffeine:local .
#
# The deploy/docker/docker-compose.local.yml file does both steps for you.
FROM amazoncorretto:25

WORKDIR /app

ARG JAR_FILE=target/reactive-local-caffeine-1.0.0-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
