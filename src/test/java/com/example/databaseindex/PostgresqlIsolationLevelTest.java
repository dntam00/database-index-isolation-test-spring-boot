package com.example.databaseindex;

import com.example.databaseindex.modal.PersonEntity;
import com.example.databaseindex.repo.PersonRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@SpringBootTest
@Transactional
@EnableTransactionManagement
@ActiveProfiles("postgresql")
@Slf4j
public class PostgresqlIsolationLevelTest {

    @Autowired
    private PersonRepository personRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void setUp() {
        // Use a separate transaction template to ensure complete commit
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        txTemplate.execute(status -> {
            personRepository.deleteAll();
            personRepository.flush();

            PersonEntity person = new PersonEntity();
            person.setName("original");
            personRepository.save(person);
            personRepository.flush();

            return null;
        });
    }

    @Test
    public void testPostgres_detectsLostUpdatesInRepeatableRead() throws InterruptedException {
        // Use CountDownLatch to coordinate thread execution
        CountDownLatch tx1ReadComplete = new CountDownLatch(1);
        CountDownLatch tx1UpdateStarted = new CountDownLatch(1);
        AtomicBoolean tx2Failed = new AtomicBoolean(false);
        AtomicReference<Exception> tx2Exception = new AtomicReference<>();

        // First transaction reads, updates, but doesn't commit until T2 has tried updating
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            try {
                txTemplate.execute(status -> {
                    try {
                        // Read data
                        PersonEntity person = personRepository.findById(
                                personRepository.findAll().get(0).getId()).get();
                        log.info("TX1: Read person with name: {}", person.getName());

                        // Signal T1 completed initial read
                        tx1ReadComplete.countDown();

                        // Update data
                        person.setName("tx1-updated-value");
                        personRepository.saveAndFlush(person);
                        log.info("TX1: Updated person to: tx1-updated-value");

                        // Signal that T1 has updated
                        tx1UpdateStarted.countDown();

                        // Wait a bit to ensure T2 has time to attempt its update
                        Thread.sleep(500);

                        return null;
                    } catch (Exception e) {
                        log.error("TX1: Exception: {}", e.getMessage(), e);
                        status.setRollbackOnly();
                        return null;
                    }
                });
            } catch (Exception e) {
                log.error("TX1: Transaction failed: {}", e.getMessage());
            }
        });

        // Second transaction reads, then tries to update after T1 updates but before T1 commits
        Thread transaction2 = new Thread(() -> {
            try {
                // Wait for T1 to read the data first
                tx1ReadComplete.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    try {
                        // Read data (should be original state)
                        PersonEntity person = personRepository.findById(
                                personRepository.findAll().get(0).getId()).get();
                        log.info("TX2: Read person with name: {}", person.getName());

                        // Wait for T1 to actually update the data
                        tx1UpdateStarted.await();

                        // Try to update after T1 has updated but before T1 has committed
                        // This should cause PostgreSQL to detect a write-write conflict
                        person.setName("tx2-updated-value");
                        personRepository.saveAndFlush(person); // This should fail in PostgreSQL
                        log.info("TX2: Updated person to: tx2-updated-value");

                        return null;
                    } catch (TransactionException e) {
                        log.error("TX2: Exception during update: {}", e.getMessage());
                        status.setRollbackOnly();
                        throw e;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (Exception e) {
                tx2Failed.set(true);
                tx2Exception.set(e);
                log.info("TX2: Transaction failed with exception: {}", e.getMessage());
            }
        });

        // Start both transactions
        transaction1.start();
        transaction2.start();

        // Wait for completion
        transaction1.join();
        transaction2.join();

        // Check final state
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        String finalName = verifyTx.execute(status ->
                                                    personRepository.findAll().get(0).getName());

        log.info("Final person name: {}", finalName);

        // With PostgreSQL's REPEATABLE_READ, transaction 2 should fail
        assertTrue(tx2Failed.get(), "Transaction 2 should have failed with a serialization error");
        assertTrue(tx2Exception.get().getMessage().contains("could not serialize") ||
                           tx2Exception.get() instanceof CannotAcquireLockException,
                   "Should fail with serialization error");
        assertEquals("tx1-updated-value", finalName, "Person should have TX1's update");
    }
}