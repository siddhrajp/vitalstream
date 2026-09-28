package com.vitalstream.api.patient;

import com.vitalstream.api.common.ConflictException;
import com.vitalstream.api.common.NotFoundException;
import com.vitalstream.api.patient.PatientDtos.PatientRequest;
import com.vitalstream.api.patient.PatientDtos.PatientResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional
public class PatientService {

    private final PatientRepository patients;

    public PatientService(PatientRepository patients) {
        this.patients = patients;
    }

    @Transactional(readOnly = true)
    public List<PatientResponse> findAll() {
        return patients.findAll().stream().map(PatientResponse::from).toList();
    }

    @Transactional(readOnly = true)
    public PatientResponse findById(Long id) {
        return PatientResponse.from(getOrThrow(id));
    }

    public PatientResponse create(PatientRequest req) {
        if (patients.existsByMrn(req.mrn())) {
            throw new ConflictException("Patient with MRN " + req.mrn() + " already exists");
        }
        Patient saved = patients.save(
                new Patient(req.mrn(), req.firstName(), req.lastName(), req.dateOfBirth()));
        return PatientResponse.from(saved);
    }

    public PatientResponse update(Long id, PatientRequest req) {
        Patient patient = getOrThrow(id);
        if (!patient.getMrn().equals(req.mrn()) && patients.existsByMrn(req.mrn())) {
            throw new ConflictException("Patient with MRN " + req.mrn() + " already exists");
        }
        patient.setMrn(req.mrn());
        patient.setFirstName(req.firstName());
        patient.setLastName(req.lastName());
        patient.setDateOfBirth(req.dateOfBirth());
        // No save() needed: the entity is "managed" inside this transaction, so Hibernate
        // detects the changes and issues an UPDATE on commit (dirty checking).
        // flush() sends that UPDATE now so the returned updatedAt is current.
        patients.flush();
        return PatientResponse.from(patient);
    }

    public void delete(Long id) {
        // Devices are removed by the ON DELETE CASCADE foreign key in V2.
        patients.delete(getOrThrow(id));
    }

    Patient getOrThrow(Long id) {
        return patients.findById(id)
                .orElseThrow(() -> new NotFoundException("Patient " + id + " not found"));
    }
}
