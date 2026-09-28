package com.vitalstream.api.reading;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReadingController {

    private final ReadingService service;

    public ReadingController(ReadingService service) {
        this.service = service;
    }

    /**
     * 202 Accepted rather than 201 Created: the reading is in Kafka, but nothing has stored it
     * in the database yet. That happens later, in a consumer.
     */
    @PostMapping("/api/devices/{deviceId}/readings")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ReadingAccepted submit(@PathVariable Long deviceId, @Valid @RequestBody ReadingRequest req,
                                  Authentication caller) {
        // Spring passes in the authenticated caller for a parameter of type Authentication; its name is
        // the token's preferred_username (see SecurityConfig.keycloakRoles).
        return service.publish(deviceId, req, caller.getName());
    }
}
