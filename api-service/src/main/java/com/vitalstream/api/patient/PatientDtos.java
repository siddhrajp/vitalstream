package com.vitalstream.api.patient;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Past;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Request/response shapes for the patient API, kept separate from the entity so the
 * JSON contract doesn't change whenever the database mapping does.
 */
public final class PatientDtos {

    private PatientDtos() {
    }

    public record PatientRequest(
            @NotBlank @Size(max = 32) String mrn,
            @NotBlank @Size(max = 100) String firstName,
            @NotBlank @Size(max = 100) String lastName,
            @NotNull @Past LocalDate dateOfBirth) {
    }

    public record PatientResponse(
            Long id,
            String mrn,
            String firstName,
            String lastName,
            LocalDate dateOfBirth,
            Instant createdAt,
            Instant updatedAt) {

        static PatientResponse from(Patient p) {
            return new PatientResponse(p.getId(), p.getMrn(), p.getFirstName(), p.getLastName(),
                    p.getDateOfBirth(), p.getCreatedAt(), p.getUpdatedAt());
        }
    }
}
