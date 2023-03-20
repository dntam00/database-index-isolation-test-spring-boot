package com.example.databaseindex.repo;

import com.example.databaseindex.modal.PersonEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonRepository extends JpaRepository<PersonEntity, Long> {
}
