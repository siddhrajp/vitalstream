package com.vitalstream.api.patient;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data generates the implementation at runtime: save, findById, findAll, deleteById, etc.
 */
public interface PatientRepository extends JpaRepository<Patient, Long> {

    boolean existsByMrn(String mrn);
}
