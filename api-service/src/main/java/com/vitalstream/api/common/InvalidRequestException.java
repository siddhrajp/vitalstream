package com.vitalstream.api.common;

/** The request is well-formed but its content is invalid: 400 over REST, INVALID_ARGUMENT over gRPC. */
public class InvalidRequestException extends RuntimeException {

    public InvalidRequestException(String message) {
        super(message);
    }
}
