package com.vitalstream.api.ingest;

import com.vitalstream.api.common.ConflictException;
import com.vitalstream.api.common.NotFoundException;
import com.vitalstream.api.common.ServiceUnavailableException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.springframework.grpc.server.exception.GrpcExceptionHandler;
import org.springframework.stereotype.Component;

/**
 * gRPC counterpart of GlobalExceptionHandler: turns the exceptions ReadingService throws into gRPC
 * status codes, so the same failure means the same thing over REST and gRPC.
 *
 *   REST 404  -> NOT_FOUND
 *   REST 409  -> FAILED_PRECONDITION   (the request is fine, but the device is in the wrong state)
 *   REST 503  -> UNAVAILABLE           (clients may retry; gRPC's retry policy treats this code as retryable)
 *
 * Returning null means "not mine", and the client gets UNKNOWN without internal details leaking out.
 */
@Component
public class GrpcErrorMapping implements GrpcExceptionHandler {

    /** Used for unary calls: Spring gRPC calls this when a service method throws. */
    @Override
    public StatusException handleException(Throwable exception) {
        Status status = statusFor(exception);
        return status == null ? null : status.asException();
    }

    /**
     * Shared with the streaming RPC, which reports errors per reading inside ReadingAck instead of
     * failing the whole stream. Returns null for unexpected exceptions (bugs), which callers must not
     * describe to the client.
     */
    static Status statusFor(Throwable exception) {
        return switch (exception) {
            case StatusRuntimeException e -> e.getStatus();   // already a gRPC status (validation errors)
            case NotFoundException e -> Status.NOT_FOUND.withDescription(e.getMessage());
            case ConflictException e -> Status.FAILED_PRECONDITION.withDescription(e.getMessage());
            case ServiceUnavailableException e -> Status.UNAVAILABLE.withDescription(e.getMessage());
            default -> null;
        };
    }
}
