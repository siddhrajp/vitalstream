# VitalStream

Learning project: an event-driven medical-device data platform built to learn
Java/Spring Boot, Kafka, Avro, gRPC, PostgreSQL/Flyway, CQRS, and Keycloak.

## How to work with me
- I'm learning this stack (I come from Python/ML). Explain new concepts briefly
  when you introduce them, and why a design choice was made.
- Build in small steps. After each step, tell me how to run and verify it.
- Don't jump ahead to later phases unless I ask.

## Stack
- Java 21, Spring Boot 3, Maven
- Docker Compose for all infrastructure
- Python for the device simulator

## Phases
1. Spring Boot REST + PostgreSQL + Flyway + JPA  (done)
2. Kafka producer/consumer  (done)
3. Avro + Schema Registry  (done)
4. gRPC ingest + Python simulator  <- current
5. CQRS read models + query tuning
6. Keycloak OIDC/JWT + role-based access
7. CI (GitHub Actions + Testcontainers) + Kubernetes