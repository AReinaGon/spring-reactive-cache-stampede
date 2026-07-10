# Runtime image for the distributed module. It runs a PREBUILT jar so the build uses your local
# Corretto 25 + Maven toolchain (no JDK-25 Maven base image to chase). Build order:
#
#   mvn -DskipTests package                       # produces target/reactive-distributed-redis-*.jar
#   docker build -t reactive-distributed-redis:local .
#
# The deploy/docker compose file performs both steps for you (build: context points here).
FROM amazoncorretto:25

WORKDIR /app

# Single repackaged Spring Boot jar (the *.jar.original left by the plugin does not match this glob).
ARG JAR_FILE=target/reactive-distributed-redis-1.0.0-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar

EXPOSE 8080

# POD_NAME labels which pod served a request (model.handledByPod). Left unset, each container gets a
# distinct random id — exactly the per-pod identity the stampede demo wants. JAVA_OPTS is pass-through.
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
