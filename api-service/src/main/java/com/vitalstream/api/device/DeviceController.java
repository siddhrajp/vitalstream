package com.vitalstream.api.device;

import com.vitalstream.api.device.DeviceDtos.DeviceRequest;
import com.vitalstream.api.device.DeviceDtos.DeviceResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api")
public class DeviceController {

    private final DeviceService service;

    public DeviceController(DeviceService service) {
        this.service = service;
    }

    @GetMapping("/devices")
    public List<DeviceResponse> list() {
        return service.findAll();
    }

    @GetMapping("/patients/{patientId}/devices")
    public List<DeviceResponse> listForPatient(@PathVariable Long patientId) {
        return service.findByPatient(patientId);
    }

    @GetMapping("/devices/{id}")
    public DeviceResponse get(@PathVariable Long id) {
        return service.findById(id);
    }

    @PostMapping("/devices")
    public ResponseEntity<DeviceResponse> create(@Valid @RequestBody DeviceRequest req) {
        DeviceResponse created = service.create(req);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(created.id()).toUri();
        return ResponseEntity.created(location).body(created);
    }

    @PutMapping("/devices/{id}")
    public DeviceResponse update(@PathVariable Long id, @Valid @RequestBody DeviceRequest req) {
        return service.update(id, req);
    }

    @DeleteMapping("/devices/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable Long id) {
        service.delete(id);
    }
}
