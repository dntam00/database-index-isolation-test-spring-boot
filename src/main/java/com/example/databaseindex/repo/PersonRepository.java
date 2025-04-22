package com.example.databaseindex.repo;

import com.example.databaseindex.modal.PersonEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import javax.persistence.LockModeType;
import java.util.List;

public interface PersonRepository extends JpaRepository<PersonEntity, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM PersonEntity p")
    List<PersonEntity> findAllForUpdate();

    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query("SELECT p FROM PersonEntity p")
    List<PersonEntity> findAllForUpdateReadLock();
}
