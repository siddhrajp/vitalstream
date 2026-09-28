package com.vitalstream.api.device;

import com.vitalstream.api.common.ConflictException;
import com.vitalstream.api.common.NotFoundException;
import com.vitalstream.api.device.DeviceDtos.DeviceRequest;
import com.vitalstream.api.device.DeviceDtos.DeviceResponse;
import com.vitalstream.api.patient.Patient;
import com.vitalstream.api.patient.PatientRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional
public class DeviceService {

    private final DeviceRepository devices;
    private final PatientRepository patients;

    public DeviceService(DeviceRepository devices, PatientRepository patients) {
        this.devices = devices;
        this.patients = patients;
    }

    @Transactional(readOnly = true)
    public List<DeviceResponse> findAll() {
        return devices.findAll().stream().map(DeviceResponse::from).toList();
    }

    @Transactional(readOnly = true)
    public List<DeviceResponse> findByPatient(Long patientId) {
        if (!patients.existsById(patientId)) {
            throw new NotFoundException("Patient " + patientId + " not found");
        }
        return devices.findByPatientId(patientId).stream().map(DeviceResponse::from).toList();
    }

    @Transactional(readOnly = true)
    public DeviceResponse findById(Long id) {
        return DeviceResponse.from(getOrThrow(id));
    }

    public DeviceResponse create(DeviceRequest req) {
        if (devices.existsBySerialNumber(req.serialNumber())) {
            throw new ConflictException("Device with serial " + req.serialNumber() + " already exists");
        }
        DeviceStatus status = req.status() != null ? req.status() : DeviceStatus.ACTIVE;
        Device device = new Device(req.serialNumber(), req.deviceType(), status, getPatient(req.patientId()));
        device.setFirmwareVersion(req.firmwareVersion());
        return DeviceResponse.from(devices.save(device));
    }

    public DeviceResponse update(Long id, DeviceRequest req) {
        Device device = getOrThrow(id);
        if (!device.getSerialNumber().equals(req.serialNumber())
                && devices.existsBySerialNumber(req.serialNumber())) {
            throw new ConflictException("Device with serial " + req.serialNumber() + " already exists");
        }
        device.setSerialNumber(req.serialNumber());
        device.setDeviceType(req.deviceType());
        if (req.status() != null) {
            device.setStatus(req.status());
        }
        device.setFirmwareVersion(req.firmwareVersion());
        device.setPatient(getPatient(req.patientId())); // reassigning a device to another patient
        devices.flush();
        return DeviceResponse.from(device);
    }

    public void delete(Long id) {
        devices.delete(getOrThrow(id));
    }

    private Device getOrThrow(Long id) {
        return devices.findById(id)
                .orElseThrow(() -> new NotFoundException("Device " + id + " not found"));
    }

    private Patient getPatient(Long patientId) {
        return patients.findById(patientId)
                .orElseThrow(() -> new NotFoundException("Patient " + patientId + " not found"));
    }
}
