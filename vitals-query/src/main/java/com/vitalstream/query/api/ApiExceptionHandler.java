package com.vitalstream.query.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;

/** Bad query parameters become 400 responses in the same problem-details format as api-service. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    /** e.g. from=yesterday instead of an ISO-8601 instant such as 2026-09-28T15:00:00Z */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail badParameter(MethodArgumentTypeMismatchException ex) {
        String hint = ex.getRequiredType() == Instant.class ? "; expected e.g. 2026-09-28T15:00:00Z" : "";
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Invalid value '" + ex.getValue() + "' for '" + ex.getName() + "'" + hint);
    }
}
