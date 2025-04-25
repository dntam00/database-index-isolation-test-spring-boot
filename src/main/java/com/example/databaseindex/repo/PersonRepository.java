package com.example.databaseindex.repo;

import com.example.databaseindex.modal.PersonEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import javax.persistence.LockModeType;
import java.util.List;
import java.util.Optional;

public interface PersonRepository extends JpaRepository<PersonEntity, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM PersonEntity p")
    List<PersonEntity> findAllForUpdate();

    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query("SELECT p FROM PersonEntity p")
    List<PersonEntity> findAllForUpdateReadLock();

    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query("SELECT p FROM PersonEntity p WHERE p.id > 200")
    List<PersonEntity> findGreaterForUpdateReadLock();

    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query("SELECT p FROM PersonEntity p WHERE p.id > 0")
    List<PersonEntity> findGreater();

    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query("SELECT p FROM PersonEntity p WHERE p.id > 0 AND p.id < 7")
    List<PersonEntity> findGreaterAndSmaller();

    @Lock(LockModeType.PESSIMISTIC_WRITE)

    @Query(value = "SELECT p FROM PersonEntity p WHERE p.id = :id")
    Optional<PersonEntity> findByIdForUpdate(@Param("id") Long id);
}
