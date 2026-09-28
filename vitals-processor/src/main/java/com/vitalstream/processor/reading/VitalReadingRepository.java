package com.vitalstream.processor.reading;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface VitalReadingRepository extends JpaRepository<VitalReading, Long> {

    boolean existsByEventId(UUID eventId);
}
