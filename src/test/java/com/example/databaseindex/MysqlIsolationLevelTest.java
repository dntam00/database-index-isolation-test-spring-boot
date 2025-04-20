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
class MysqlIsolationLevelTest {

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

    @Test
    public void testReadCommitted_shouldOnlyReadCommittedData() throws InterruptedException {
        // Number of update threads
        final int NUM_THREADS = 5;

        // Use a CountDownLatch to signal when the commit happens
        CountDownLatch commitSignal = new CountDownLatch(1);

        // Track which thread will commit (randomly chosen)
        int committingThreadIndex = (int) (Math.random() * NUM_THREADS);
        AtomicReference<String> expectedCommittedValue = new AtomicReference<>();

        // Create and start all update threads
        Thread[] updateThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            final String newValue = "modified-by-thread-" + threadIndex;

            // If this is the thread that will commit, remember its value
            if (threadIndex == committingThreadIndex) {
                expectedCommittedValue.set(newValue);
            }

            updateThreads[i] = new Thread(() -> {
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    PersonEntity person = personRepository.findAll().get(0);
                    person.setName(newValue);
                    personRepository.saveAndFlush(person);

                    // Only commit if this is the designated thread
                    if (threadIndex == committingThreadIndex) {
                        // Signal that the commit is happening
                        commitSignal.countDown();
                        return null;
                    } else {
                        // Roll back all other transactions
                        status.setRollbackOnly();
                        return null;
                    }
                });
            });

            updateThreads[i].start();
        }

        // Create and start the reader thread
        AtomicReference<String> actualReadValue = new AtomicReference<>();
        Thread readerThread = new Thread(() -> {
            try {
                // Wait for the commit signal
                commitSignal.await();

                // Add a small delay to ensure the commit has completed
                Thread.sleep(200);

                // Read with default isolation (READ_COMMITTED)
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    PersonEntity person = personRepository.findAll().get(0);
                    actualReadValue.set(person.getName());
                    return null;
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        readerThread.start();

        // Wait for all threads to complete
        for (Thread thread : updateThreads) {
            thread.join();
        }
        readerThread.join();

        // Verify that only the committed value was read
        assertEquals(expectedCommittedValue.get(), actualReadValue.get(),
                     "Should only read the value from the committed transaction");
    }
}