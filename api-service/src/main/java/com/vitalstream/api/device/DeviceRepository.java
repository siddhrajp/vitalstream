package com.vitalstream.api.device;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DeviceRepository extends JpaRepository<Device, Long> {

    // Spring Data derives the query from the method name: WHERE patient.id = ?
    List<Device> findByPatientId(Long patientId);

    boolean existsBySerialNumber(String serialNumber);
}
