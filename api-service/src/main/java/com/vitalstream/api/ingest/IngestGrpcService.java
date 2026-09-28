package com.vitalstream.api.ingest;

import com.google.protobuf.Timestamp;
import com.vitalstream.api.reading.Metric;
import com.vitalstream.api.reading.ReadingAccepted;
import com.vitalstream.api.reading.ReadingRequest;
import com.vitalstream.api.reading.ReadingService;
import com.vitalstream.ingest.v1.IngestServiceGrpc;
import com.vitalstream.ingest.v1.SendReadingRequest;
import com.vitalstream.ingest.v1.SendReadingResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * gRPC version of POST /api/devices/{id}/readings. Both end up in the same ReadingService, so the device
 * checks and Kafka publishing are identical; only the transport and error format differ.
 *
 * IngestServiceImplBase is generated from ingest.proto. Spring gRPC finds this bean and serves it on the
 * gRPC port (spring.grpc.server.port).
 */
@Service
public class IngestGrpcService extends IngestServiceGrpc.IngestServiceImplBase {

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
        ReadingAccepted accepted = readings.publish(request.getDeviceId(), toReadingRequest(request));
        responseObserver.onNext(SendReadingResponse.newBuilder().setEventId(accepted.eventId()).build());
        responseObserver.onCompleted();
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
            if (measuredAt.isAfter(Instant.now())) {
                throw invalid("measured_at must not be in the future");
            }
        }
        return new ReadingRequest(metric, request.getValue(), measuredAt);
    }

    /** A StatusRuntimeException already carries its gRPC status, so it's sent to the client as-is. */
    private static RuntimeException invalid(String message) {
        return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }
}
