package com.vitalstream.api.ingest;

import com.vitalstream.api.common.ConflictException;
import com.vitalstream.api.common.NotFoundException;
import com.vitalstream.api.common.ServiceUnavailableException;
import io.grpc.Status;
import io.grpc.StatusException;
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

    @Override
    public StatusException handleException(Throwable exception) {
        Status status = switch (exception) {
            case NotFoundException e -> Status.NOT_FOUND;
            case ConflictException e -> Status.FAILED_PRECONDITION;
            case ServiceUnavailableException e -> Status.UNAVAILABLE;
            default -> null;
        };
        return status == null ? null : status.withDescription(exception.getMessage()).asException();
    }
}
