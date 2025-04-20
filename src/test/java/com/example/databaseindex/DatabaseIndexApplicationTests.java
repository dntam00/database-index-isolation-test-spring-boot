package com.example.databaseindex;

import com.example.databaseindex.modal.PersonEntity;
import com.example.databaseindex.repo.PersonRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@EnableTransactionManagement
@Transactional
@ActiveProfiles("mysql")
class ReadUncommittedIsolationTest {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private PersonRepository personRepository;

    @BeforeEach
    void setUp() {
        // Use a separate transaction template to ensure complete commit
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW); // Force a new transaction

        txTemplate.execute(status -> {
            personRepository.deleteAll();
            personRepository.flush(); // Flush the delete operation

            PersonEntity person = new PersonEntity();
            person.setName("original");
            personRepository.save(person);
            personRepository.flush(); // Explicitly flush to ensure persistence

            return null;
        });

        // Verify data was committed by checking outside transaction
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        verifyTx.execute(status -> {
            assert personRepository.count() == 1 : "Setup data wasn't properly committed";
            return null;
        });
    }

    @Test
    public void testReadUncommitted_shouldReadDirtyData() throws InterruptedException {
        // Create a latch to coordinate between threads
        CountDownLatch updateStarted = new CountDownLatch(1);
        CountDownLatch readCompleted = new CountDownLatch(1);

        // Thread for second transaction (will read uncommitted data)
        AtomicReference<String> readValue = new AtomicReference<>();
        AtomicBoolean dirtyReadConfirmed = new AtomicBoolean(false);

        Thread readerThread = new Thread(() -> {
            try {
                updateStarted.await(); // Wait for writer to start but not commit

                // Configure transaction with READ_UNCOMMITTED
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);

                txTemplate.execute(status -> {
                    PersonEntity person = personRepository.findAll().get(0);
                    readValue.set(person.getName());

                    // If we can read "modified" before the other transaction commits,
                    // that confirms a dirty read
                    dirtyReadConfirmed.set("modified".equals(person.getName()));
                    return null;
                });

                readCompleted.countDown(); // Signal that reading is done
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Thread for first transaction (will modify but not commit until after read)
        Thread writerThread = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);

            txTemplate.execute(status -> {
                PersonEntity person = personRepository.findAll().get(0);
                person.setName("modified");
                personRepository.save(person);
                personRepository.flush();

                // Signal that we've updated but not yet committed
                updateStarted.countDown();

                try {
                    // Wait for reader to complete before committing
                    readCompleted.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                return null;
            });
        });

        // Start both threads
        writerThread.start();
        readerThread.start();

        // Wait for both threads to complete
        writerThread.join();
        readerThread.join();

        // Verify that a dirty read occurred
        assertEquals("modified", readValue.get());
        assertTrue(dirtyReadConfirmed.get(), "Should have read uncommitted data (dirty read)");
    }
}