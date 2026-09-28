package com.vitalstream.api.device;

import com.vitalstream.api.patient.Patient;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;

@Entity
@Table(name = "devices")
public class Device {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "serial_number", nullable = false, unique = true, length = 64)
    private String serialNumber;

    @Enumerated(EnumType.STRING) // store "HEART_RATE_MONITOR", not the ordinal 0/1/2
    @Column(name = "device_type", nullable = false, length = 32)
    private DeviceType deviceType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private DeviceStatus status;

    @Column(name = "firmware_version", length = 32)
    private String firmwareVersion;

    // Many devices -> one patient. LAZY means the patient row is only loaded when accessed.
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "patient_id", nullable = false)
    private Patient patient;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Device() {
        // required by JPA
    }

    public Device(String serialNumber, DeviceType deviceType, DeviceStatus status, Patient patient) {
        this.serialNumber = serialNumber;
        this.deviceType = deviceType;
        this.status = status;
        this.patient = patient;
    }

    public Long getId() { return id; }
    public String getSerialNumber() { return serialNumber; }
    public DeviceType getDeviceType() { return deviceType; }
    public DeviceStatus getStatus() { return status; }
    public String getFirmwareVersion() { return firmwareVersion; }
    public Patient getPatient() { return patient; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void setSerialNumber(String serialNumber) { this.serialNumber = serialNumber; }
    public void setDeviceType(DeviceType deviceType) { this.deviceType = deviceType; }
    public void setStatus(DeviceStatus status) { this.status = status; }
    public void setFirmwareVersion(String firmwareVersion) { this.firmwareVersion = firmwareVersion; }
    public void setPatient(Patient patient) { this.patient = patient; }
}
