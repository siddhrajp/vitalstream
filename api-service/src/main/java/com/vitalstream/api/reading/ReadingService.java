package com.vitalstream.api.reading;

import com.vitalstream.api.common.ConflictException;
import com.vitalstream.api.common.InvalidRequestException;
import com.vitalstream.api.common.NotFoundException;
import com.vitalstream.api.common.ServiceUnavailableException;
import com.vitalstream.api.device.Device;
import com.vitalstream.api.device.DeviceRepository;
import com.vitalstream.api.device.DeviceStatus;
import com.vitalstream.events.VitalReadingEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class ReadingService {

    private static final Logger log = LoggerFactory.getLogger(ReadingService.class);
    private static final Duration MAX_CLOCK_SKEW = Duration.ofSeconds(5);

    private final DeviceRepository devices;
    private final KafkaTemplate<String, VitalReadingEvent> kafka;
    private final String topic;

    public ReadingService(DeviceRepository devices,
                          KafkaTemplate<String, VitalReadingEvent> kafka,
                          @Value("${vitalstream.kafka.topics.readings}") String topic) {
        this.devices = devices;
        this.kafka = kafka;
        this.topic = topic;
    }

    /**
     * @param submittedBy who sent the reading (the authenticated caller's name). Passed in explicitly by each
     *                    entry point (REST controller, gRPC service) rather than read from a thread-local
     *                    security context in here, so it's visible where the value comes from.
     */
    public ReadingAccepted publish(Long deviceId, ReadingRequest req, String submittedBy) {
        Device device = devices.findById(deviceId)
                .orElseThrow(() -> new NotFoundException("Device " + deviceId + " not found"));
        if (device.getStatus() != DeviceStatus.ACTIVE) {
            throw new ConflictException("Device " + deviceId + " is " + device.getStatus()
                    + "; only ACTIVE devices can send readings");
        }

        // The schema stores timestamps in milliseconds, so truncate now; otherwise the response would
        // show microseconds that the event doesn't actually carry.
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant measuredAt = req.measuredAt() != null ? req.measuredAt().truncatedTo(ChronoUnit.MILLIS) : now;
        // Device clocks are never exactly in sync with ours: a reading taken "just now" by a device whose clock
        // runs a few milliseconds fast looks like it's from the future. Allow some skew; reject real nonsense.
        if (measuredAt.isAfter(now.plus(MAX_CLOCK_SKEW))) {
            throw new InvalidRequestException("measuredAt is more than " + MAX_CLOCK_SKEW.toSeconds()
                    + "s in the future: " + measuredAt);
        }

        // VitalReadingEvent is generated from schemas/avro/VitalReadingEvent.avsc. build() fails if a
        // field without a default is missing, so an incomplete event can't be sent.
        VitalReadingEvent event = VitalReadingEvent.newBuilder()
                // The client's readingId if given, so a retried reading keeps the same event id and the
                // processor's duplicate check (by event id) stores it only once.
                .setEventId((req.readingId() != null ? req.readingId() : UUID.randomUUID()).toString())
                .setDeviceId(deviceId)
                .setPatientId(device.getPatient().getId())
                .setMetric(com.vitalstream.events.Metric.valueOf(req.metric().name()))
                .setValue(req.value())
                .setMeasuredAt(measuredAt)
                .setReceivedAt(now)
                .setFirmwareVersion(device.getFirmwareVersion())
                .setSubmittedBy(submittedBy)
                .build();

        // Key = device id: every reading from one device goes to the same partition, so they stay in order.
        String key = deviceId.toString();
        try {
            // send() is asynchronous and returns a future. We wait for the broker's acknowledgement so that
            // a 202 response means the reading is safely stored in Kafka, not just queued in this process.
            RecordMetadata meta = kafka.send(topic, key, event)
                    .get(15, TimeUnit.SECONDS)
                    .getRecordMetadata();
            log.debug("Published reading {} for device {} to {}-{} at offset {}",
                    event.getEventId(), deviceId, meta.topic(), meta.partition(), meta.offset());
            return ReadingAccepted.from(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceUnavailableException("Interrupted while publishing reading");
        } catch (ExecutionException | TimeoutException | KafkaException
                 | org.apache.kafka.common.KafkaException e) {
            // The last one covers SerializationException, thrown synchronously by send() when the Avro
            // serializer can't reach Schema Registry to register or look up the schema.
            log.error("Failed to publish reading for device {}", deviceId, e);
            throw new ServiceUnavailableException("Reading could not be published; try again later");
        }
    }
}
