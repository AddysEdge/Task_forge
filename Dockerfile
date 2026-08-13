# ---------- build ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Dependencies are resolved from the pom alone first, so editing source code does not
# invalidate the (large, slow) dependency layer.
COPY pom.xml .
RUN mvn -B -ntp dependency:go-offline

COPY src ./src
# Tests run in CI against real containers, not during the image build.
RUN mvn -B -ntp package -DskipTests

# ---------- runtime ----------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Unprivileged user: nothing in TaskForge needs root.
RUN addgroup -S taskforge && adduser -S -G taskforge taskforge

COPY --from=build /build/target/taskforge-*.jar /app/app.jar
RUN chown -R taskforge:taskforge /app

USER taskforge
EXPOSE 8080

# Both roles serve actuator, so API and workers use the same probe.
HEALTHCHECK --interval=10s --timeout=5s --start-period=90s --retries=12 \
    CMD wget -q -O - http://localhost:8080/actuator/health | grep -q '"status":"UP"' || exit 1

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
