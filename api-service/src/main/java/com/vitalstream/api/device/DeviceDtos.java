package com.vitalstream.api.device;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public final class DeviceDtos {

    private DeviceDtos() {
    }

    /** status is optional on create and defaults to ACTIVE; firmwareVersion is optional. */
    public record DeviceRequest(
            @NotBlank @Size(max = 64) String serialNumber,
            @NotNull DeviceType deviceType,
            DeviceStatus status,
            @Size(max = 32) String firmwareVersion,
            @NotNull Long patientId) {
    }

    public record DeviceResponse(
            Long id,
            String serialNumber,
            DeviceType deviceType,
            DeviceStatus status,
            String firmwareVersion,
            Long patientId,
            Instant createdAt,
            Instant updatedAt) {

        static DeviceResponse from(Device d) {
            // getPatient().getId() does not trigger a lazy load: Hibernate already knows the FK value.
            return new DeviceResponse(d.getId(), d.getSerialNumber(), d.getDeviceType(), d.getStatus(),
                    d.getFirmwareVersion(), d.getPatient().getId(), d.getCreatedAt(), d.getUpdatedAt());
        }
    }
}
