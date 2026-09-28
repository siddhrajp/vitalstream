package com.vitalstream.processor.reading;

/** A message that can never be processed, no matter how often it is retried. */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
