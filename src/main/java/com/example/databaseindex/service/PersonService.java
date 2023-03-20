package com.example.databaseindex.service;

import com.example.databaseindex.modal.PersonEntity;
import com.example.databaseindex.repo.PersonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class PersonService {
    
    private final PersonRepository personRepository;
    private final EntityManager entityManager;
    
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public Pair<PersonEntity, PersonEntity> readCommittedPerson(Long id) {
        var before = personRepository.findById(id).orElse(new PersonEntity());
        entityManager.clear();
        sleep(5000);
        var after = personRepository.findById(id).orElse(new PersonEntity());
        return Pair.of(before, after);
    }
    
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Pair<List<PersonEntity>, List<PersonEntity>> readPersons() {
        var before = personRepository.findAll();
        entityManager.clear();
        sleep(3000);
        var after = personRepository.findAll();
        return Pair.of(before, after);
    }
    
    /**
     * Note:
     * <p>In postgresql, REPEATABLE_READ doesn't allow phantom phenomenon.</p>
     * <p>Sql standard only defines what phenomenon is not allowed in what database isolation level</p>
    * */
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public Pair<List<PersonEntity>, List<PersonEntity>> readPersonsWithRepeatableRead() {
        var before = personRepository.findAll();
        entityManager.clear();
        sleep(3000);
        var after = personRepository.findAll();
        return Pair.of(before, after);
    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void updatePerson(Long id) {
        sleep(2000);
        var personEntity = personRepository.findById(id).orElse(new PersonEntity());
        personEntity.setName("dnt updated");
        personRepository.saveAndFlush(personEntity);
    }
    
    @Transactional
    public void addPerson() {
        sleep(1000);
        var personEntity = new PersonEntity();
        personEntity.setName("dnt_3");
        personRepository.saveAndFlush(personEntity);
        entityManager.clear();
    }
    
    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            log.debug("Error while sleeping thread", ex);
            Thread.currentThread().interrupt();
        }
    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void init() {
        personRepository.deleteAll();
        personRepository.flush();
        PersonEntity person1 = new PersonEntity();
        person1.setName("dnt_1");
        PersonEntity person2 = new PersonEntity();
        person2.setName("dnt_2");
        personRepository.saveAllAndFlush(List.of(person1, person2));
    }
}
