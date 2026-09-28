package com.vitalstream.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitalstream.events.Metric;
import com.vitalstream.events.VitalReadingEvent;
import com.vitalstream.api.security.SecurityConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end tests of api-service against a real PostgreSQL and a real Kafka broker, each started in a
 * throwaway Docker container by Testcontainers (Docker must be running). Flyway runs the real migrations
 * against the real database, and published readings are read back from the real topic.
 *
 * Two things are replaced, so the tests don't need the rest of the infrastructure:
 *  - Schema Registry: the Avro serializer's built-in in-memory registry ("mock://..." URL).
 *  - Keycloak: spring-security-test's jwt() hands the app a ready-made token with chosen claims, skipping
 *    signature checks. The role mapping (realm_access.roles -> ROLE_...) is still the app's own code.
 */
@SpringBootTest(properties = {
        "spring.kafka.producer.properties.schema.registry.url=" + ApiServiceIntegrationTest.MOCK_REGISTRY,
        "spring.grpc.server.port=0"  // any free port, so tests don't clash with a locally running api-service
})
@AutoConfigureMockMvc
@Testcontainers
class ApiServiceIntegrationTest {

    static final String MOCK_REGISTRY = "mock://api-service-it";

    // @Container: started once before the tests in this class, removed afterwards.
    // @ServiceConnection: Spring Boot points the app's datasource / Kafka settings at the container.
    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    // The app's issuer-uri is only contacted when a real token has to be verified, which never happens here
    // (jwt() bypasses verification), so a dummy value is fine.
    @DynamicPropertySource
    static void noKeycloak(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> "http://unused.invalid/realms/test");
    }

    @Autowired
    MockMvc mvc;

    @Autowired
    ObjectMapper json;

    @Test
    void requestsNeedATokenAndTheRightRole() throws Exception {
        mvc.perform(get("/api/patients")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/patients").with(caller("alice", "clinician"))).andExpect(status().isOk());
        mvc.perform(post("/api/patients").with(caller("alice", "clinician"))
                        .contentType(MediaType.APPLICATION_JSON).content(patientJson("MRN-IT-ROLE")))
                .andExpect(status().isForbidden());
        mvc.perform(get("/api/devices").with(caller("device-1", "device"))).andExpect(status().isForbidden());
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }

    @Test
    void adminRegistersPatientAndDeviceInPostgres() throws Exception {
        long patientId = createPatient("MRN-IT-ADMIN");
        long deviceId = createDevice("SER-IT-ADMIN", patientId);

        JsonNode device = json.readTree(mvc.perform(get("/api/devices/" + deviceId).with(caller("alice", "clinician")))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(device.get("patientId").asLong()).isEqualTo(patientId);
        assertThat(device.get("status").asText()).isEqualTo("ACTIVE");
    }

    @Test
    void deviceReadingIsPublishedToKafkaAsAvroWithSubmitter() throws Exception {
        long patientId = createPatient("MRN-IT-READING");
        long deviceId = createDevice("SER-IT-READING", patientId);
        UUID readingId = UUID.randomUUID();

        mvc.perform(post("/api/devices/" + deviceId + "/readings").with(caller("service-account-test", "device"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"metric\":\"HEART_RATE\",\"value\":71,\"readingId\":\"" + readingId + "\"}"))
                .andExpect(status().isAccepted());

        VitalReadingEvent event = readEventFromKafka(readingId.toString());
        assertThat(event.getDeviceId()).isEqualTo(deviceId);
        assertThat(event.getPatientId()).isEqualTo(patientId);
        assertThat(event.getMetric()).isEqualTo(Metric.HEART_RATE);
        assertThat(event.getValue()).isEqualTo(71.0);
        assertThat(event.getSubmittedBy()).isEqualTo("service-account-test");
    }

    @Test
    void smallDeviceClockSkewIsToleratedButFarFutureIsRejected() throws Exception {
        long deviceId = createDevice("SER-IT-CLOCK", createPatient("MRN-IT-CLOCK"));
        String path = "/api/devices/" + deviceId + "/readings";
        // A device whose clock runs 2 s fast: must be accepted (this used to fail ~5% of real readings).
        mvc.perform(post(path).with(caller("service-account-test", "device")).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"metric\":\"HEART_RATE\",\"value\":70,\"measuredAt\":\"" + java.time.Instant.now().plusSeconds(2) + "\"}"))
                .andExpect(status().isAccepted());
        // An hour ahead is a broken clock or bad data: rejected.
        mvc.perform(post(path).with(caller("service-account-test", "device")).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"metric\":\"HEART_RATE\",\"value\":70,\"measuredAt\":\"" + java.time.Instant.now().plusSeconds(3600) + "\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void readingsFromUnknownDevicesAreRejected() throws Exception {
        mvc.perform(post("/api/devices/999999/readings").with(caller("service-account-test", "device"))
                        .contentType(MediaType.APPLICATION_JSON).content("{\"metric\":\"HEART_RATE\",\"value\":71}"))
                .andExpect(status().isNotFound());
    }

    // ---------- helpers ----------

    /**
     * A request made by `name` holding the given Keycloak realm roles. The token carries the same claims as a
     * real Keycloak token, and its authorities come from the app's own converter.
     */
    private static RequestPostProcessor caller(String name, String... roles) {
        return jwt()
                .jwt(token -> token.subject(name).claim("preferred_username", name)
                        .claim("realm_access", Map.of("roles", List.of(roles))))
                .authorities(SecurityConfig::realmRoles);
    }

    private long createPatient(String mrn) throws Exception {
        String body = mvc.perform(post("/api/patients").with(caller("bob", "admin"))
                        .contentType(MediaType.APPLICATION_JSON).content(patientJson(mrn)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return json.readTree(body).get("id").asLong();
    }

    private long createDevice(String serial, long patientId) throws Exception {
        String body = mvc.perform(post("/api/devices").with(caller("bob", "admin"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"serialNumber\":\"" + serial + "\",\"deviceType\":\"HEART_RATE_MONITOR\",\"patientId\":" + patientId + "}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return json.readTree(body).get("id").asLong();
    }

    private static String patientJson(String mrn) {
        return "{\"mrn\":\"" + mrn + "\",\"firstName\":\"Test\",\"lastName\":\"Patient\",\"dateOfBirth\":\"1980-05-05\"}";
    }

    /** Reads the topic from the start until the event with this id shows up (or 15 seconds pass). */
    private VitalReadingEvent readEventFromKafka(String eventId) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                "schema.registry.url", MOCK_REGISTRY,
                "specific.avro.reader", true);
        try (KafkaConsumer<String, Object> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), avroDeserializer(config))) {
            consumer.subscribe(List.of("vitals.readings.avro"));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, Object> record : consumer.poll(Duration.ofMillis(500))) {
                    VitalReadingEvent event = (VitalReadingEvent) record.value();
                    if (event.getEventId().equals(eventId)) {
                        return event;
                    }
                }
            }
        }
        throw new AssertionError("No event " + eventId + " on vitals.readings.avro within 15 s");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static org.apache.kafka.common.serialization.Deserializer<Object> avroDeserializer(Map<String, Object> config) {
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
        return (org.apache.kafka.common.serialization.Deserializer) deserializer;
    }
}
