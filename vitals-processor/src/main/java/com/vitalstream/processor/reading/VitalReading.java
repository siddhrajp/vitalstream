package com.vitalstream.processor.reading;

import com.vitalstream.events.VitalReadingEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "vital_readings")
public class VitalReading {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true, updatable = false)
    private UUID eventId;

    @Column(name = "device_id", nullable = false)
    private Long deviceId;

    @Column(name = "patient_id", nullable = false)
    private Long patientId;

    @Column(nullable = false, length = 32)
    private String metric;

    @Column(nullable = false)
    private Double value;

    @Column(name = "measured_at", nullable = false)
    private Instant measuredAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "firmware_version", length = 32)
    private String firmwareVersion;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    @Column(name = "kafka_partition", nullable = false)
    private Integer kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private Long kafkaOffset;

    protected VitalReading() {
        // required by JPA
    }

    public VitalReading(VitalReadingEvent event, UUID eventId, int kafkaPartition, long kafkaOffset) {
        this.eventId = eventId;
        this.deviceId = event.getDeviceId();
        this.patientId = event.getPatientId();
        this.metric = event.getMetric().name();
        this.value = event.getValue();
        this.measuredAt = event.getMeasuredAt();
        this.receivedAt = event.getReceivedAt();
        this.firmwareVersion = event.getFirmwareVersion(); // null for events written with schema v1
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
    }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public Long getDeviceId() { return deviceId; }
    public Long getPatientId() { return patientId; }
    public String getMetric() { return metric; }
    public Double getValue() { return value; }
    public Instant getMeasuredAt() { return measuredAt; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getFirmwareVersion() { return firmwareVersion; }
    public Instant getProcessedAt() { return processedAt; }
    public Integer getKafkaPartition() { return kafkaPartition; }
    public Long getKafkaOffset() { return kafkaOffset; }
}
