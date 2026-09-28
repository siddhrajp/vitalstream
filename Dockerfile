# syntax=docker/dockerfile:1
#
# One Dockerfile for all three Java services; pick one with --build-arg SERVICE=...
# Build from the repository root, because the services generate code from ../schemas:
#
#   docker build --build-arg SERVICE=api-service      -t vitalstream/api-service .
#   docker build --build-arg SERVICE=vitals-processor -t vitalstream/vitals-processor .
#   docker build --build-arg SERVICE=vitals-query     -t vitalstream/vitals-query .
#
# Tests are not run here: CI runs them (they need Docker themselves, for Testcontainers).

ARG SERVICE

# ---------- stage 1: build (full JDK + Maven; thrown away afterwards) ----------
FROM eclipse-temurin:21-jdk AS build
ARG SERVICE
WORKDIR /workspace

COPY schemas schemas
COPY ${SERVICE}/mvnw ${SERVICE}/pom.xml ${SERVICE}/
COPY ${SERVICE}/.mvn ${SERVICE}/.mvn
COPY ${SERVICE}/src ${SERVICE}/src

WORKDIR /workspace/${SERVICE}
# --mount=type=cache keeps Maven's download folder (~/.m2) between builds, outside the image, so a code
# change doesn't download every dependency again.
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -q -DskipTests package && cp target/*-SNAPSHOT.jar /workspace/app.jar

# Split the Spring Boot jar into layers, ordered from least to most often changed:
# third-party libraries, Spring Boot's launcher, snapshot libraries, and this service's own classes.
WORKDIR /workspace
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

# ---------- stage 2: runtime (JRE only: no compiler, no Maven, no source code) ----------
FROM eclipse-temurin:21-jre

# Run as an unprivileged user: if the service were compromised, the attacker isn't root in the container.
RUN groupadd --system app && useradd --system --gid app --no-create-home app

WORKDIR /app
# One image layer per jar layer. Docker reuses unchanged layers, so after a code change only the small
# "application" layer (this service's classes) is rebuilt, pushed and pulled; the ~100 MB of libraries isn't.
COPY --from=build /workspace/extracted/dependencies/ ./
COPY --from=build /workspace/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/extracted/application/ ./

USER app
# Size the Java heap relative to the container's memory limit (Kubernetes sets one) instead of the host's.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
