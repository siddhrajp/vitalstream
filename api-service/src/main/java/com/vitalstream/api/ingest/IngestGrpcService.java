package com.vitalstream.api.ingest;

import com.google.protobuf.Timestamp;
import com.vitalstream.api.reading.Metric;
import com.vitalstream.api.reading.ReadingAccepted;
import com.vitalstream.api.reading.ReadingRequest;
import com.vitalstream.api.reading.ReadingService;
import com.vitalstream.ingest.v1.IngestServiceGrpc;
import com.vitalstream.ingest.v1.ReadingAck;
import com.vitalstream.ingest.v1.ReadingError;
import com.vitalstream.ingest.v1.SendReadingRequest;
import com.vitalstream.ingest.v1.SendReadingResponse;
import com.vitalstream.ingest.v1.StreamReadingsRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * gRPC version of POST /api/devices/{id}/readings. Both end up in the same ReadingService, so the device
 * checks and Kafka publishing are identical; only the transport and error format differ.
 *
 * IngestServiceImplBase is generated from ingest.proto. Spring gRPC finds this bean and serves it on the
 * gRPC port (spring.grpc.server.port).
 */
@Service
public class IngestGrpcService extends IngestServiceGrpc.IngestServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(IngestGrpcService.class);
    private static final String METRIC_PREFIX = "METRIC_";

    private final ReadingService readings;

    public IngestGrpcService(ReadingService readings) {
        this.readings = readings;
    }

    /**
     * gRPC methods don't return a value. The response goes to the StreamObserver: onNext() sends a message,
     * onCompleted() ends the call. That shape is shared with streaming RPCs, where onNext() is called many
     * times. If this method throws, GrpcErrorMapping turns the exception into a gRPC status code.
     */
    @Override
    public void sendReading(SendReadingRequest request, StreamObserver<SendReadingResponse> responseObserver) {
        ReadingAccepted accepted = readings.publish(request.getDeviceId(), toReadingRequest(request), callerName());
        responseObserver.onNext(SendReadingResponse.newBuilder().setEventId(accepted.eventId()).build());
        responseObserver.onCompleted();
    }

    /**
     * Bidirectional streaming. Instead of handling one request, this returns a StreamObserver that gRPC
     * calls for every message the client sends, and writes acks to `acks` as it goes. gRPC delivers one
     * stream's messages one at a time, in order, so acks come back in the order readings were sent.
     *
     * Each reading is handled independently: a failure becomes an error ack and the stream stays open.
     * If the client sends faster than readings can be published, HTTP/2 flow control makes the client's
     * sends wait ("backpressure") rather than letting messages pile up in memory here.
     */
    @Override
    public StreamObserver<StreamReadingsRequest> streamReadings(StreamObserver<ReadingAck> acks) {
        // Captured once, when the stream opens and the security interceptor has just authenticated the call.
        // onNext() below runs later, once per message, possibly on other threads, so it uses this value
        // instead of looking up the security context again.
        String caller = callerName();
        return new StreamObserver<>() {
            @Override
            public void onNext(StreamReadingsRequest request) {
                acks.onNext(acknowledge(request, caller));
            }

            @Override
            public void onError(Throwable t) {
                // The client cancelled or the connection dropped; there's no one left to answer.
                log.debug("Reading stream ended by client: {}", Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {
                // The client has finished sending; finish our side too.
                acks.onCompleted();
            }
        };
    }

    /**
     * The authenticated caller of the current gRPC call: GrpcSecurityConfig's interceptor validates the token
     * and puts the result in Spring Security's context before the service method runs.
     */
    private static String callerName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null) {
            // Can't happen while GrpcSecurityConfig requires a token for this service; fail safe if it ever does.
            throw Status.UNAUTHENTICATED.withDescription("no authenticated caller").asRuntimeException();
        }
        return auth.getName();
    }

    private ReadingAck acknowledge(StreamReadingsRequest request, String caller) {
        ReadingAck.Builder ack = ReadingAck.newBuilder().setSequence(request.getSequence());
        try {
            SendReadingRequest reading = request.getReading();
            ReadingAccepted accepted = readings.publish(reading.getDeviceId(), toReadingRequest(reading), caller);
            return ack.setEventId(accepted.eventId()).build();
        } catch (RuntimeException e) {
            Status status = GrpcErrorMapping.statusFor(e);
            if (status == null) {
                log.error("Unexpected error handling reading {}", request.getSequence(), e);
                status = Status.INTERNAL.withDescription("internal error");
            }
            return ack.setError(ReadingError.newBuilder()
                    .setCode(status.getCode().name())
                    .setMessage(status.getDescription() == null ? "" : status.getDescription()))
                    .build();
        }
    }

    /**
     * REST gets these checks from @Valid annotations on ReadingRequest; gRPC has no equivalent, so they're
     * done here. Every field in proto3 has a value (0, 0.0, UNSPECIFIED when unset), so "missing" has to be
     * detected by looking for those zero values.
     */
    private static ReadingRequest toReadingRequest(SendReadingRequest request) {
        if (request.getDeviceId() <= 0) {
            throw invalid("device_id must be positive");
        }
        Metric metric = switch (request.getMetric()) {
            case METRIC_UNSPECIFIED -> throw invalid("metric is required");
            case UNRECOGNIZED -> throw invalid("unknown metric number " + request.getMetricValue());
            default -> Metric.valueOf(request.getMetric().name().substring(METRIC_PREFIX.length()));
        };
        if (!Double.isFinite(request.getValue())) {
            throw invalid("value must be a finite number");
        }
        Instant measuredAt = null;
        if (request.hasMeasuredAt()) {
            Timestamp ts = request.getMeasuredAt();
            measuredAt = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
            // "Not in the future" is checked in ReadingService, the same rule as for REST.
        }
        UUID readingId = null;
        if (!request.getReadingId().isEmpty()) {   // proto3: an unset string reads as ""
            try {
                readingId = UUID.fromString(request.getReadingId());
            } catch (IllegalArgumentException e) {
                throw invalid("reading_id must be a UUID");
            }
        }
        return new ReadingRequest(metric, request.getValue(), measuredAt, readingId);
    }

    /** A StatusRuntimeException already carries its gRPC status, so it's sent to the client as-is. */
    private static RuntimeException invalid(String message) {
        return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }
}
