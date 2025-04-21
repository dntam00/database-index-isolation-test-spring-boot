package com.example.databaseindex;

import com.example.databaseindex.modal.PersonEntity;
import com.example.databaseindex.repo.PersonRepository;
import lombok.extern.slf4j.Slf4j;
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

import javax.persistence.EntityManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
@Slf4j
class MysqlIsolationLevelTest {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private PersonRepository personRepository;

    @Autowired
    private EntityManager entityManager;

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

    @Test
    public void testUnCommitted_shouldReadDataOfOthersTransactions() throws InterruptedException {
        // Number of update threads
        final int NUM_THREADS = 100;

        // Create and start all update threads
        Thread[] updateThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            final String newValue = "modified-by-thread-" + i;

            updateThreads[i] = new Thread(() -> {
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    PersonEntity person = personRepository.findAll().get(0);
                    person.setName(newValue);
                    personRepository.saveAndFlush(person);
                    return null;
                });
            });
        }

        // Thread for first transaction (will modify but not commit until after read)
        Thread readerThread = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);

            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);

            txTemplate.execute(status -> {
                Set<String> set = new HashSet<>();
                for (int i = 0; i < 50; i++) {
                    PersonEntity person = personRepository.findAll().get(0);
                    log.info("Reading name: {}", person.getName());
                    set.add(person.getName());
                    entityManager.clear();
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // signal that we've updated but not yet committed
                log.info("Reader thread finished reading, set size: {}", set.size());
                assertTrue(set.size() > 1, "Should not have read uncommitted data");

                return null;
            });
        });

        readerThread.start();

        for (int i = 0; i < NUM_THREADS; i++) {
            updateThreads[i].start();
        }

        // Wait for both threads to complete
        readerThread.join();
        for (Thread thread : updateThreads) {
            thread.join();
        }
    }


    @Test
    public void testRepeatableRead_shouldReadConsistentData() throws InterruptedException {
        // Number of update threads
        final int NUM_THREADS = 100;

        // Create and start all update threads
        Thread[] updateThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            final String newValue = "modified-by-thread-" + i;

            updateThreads[i] = new Thread(() -> {
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    PersonEntity person = personRepository.findAll().get(0);
                    person.setName(newValue);
                    personRepository.saveAndFlush(person);
                    return null;
                });
            });
        }

        Set<String> set = new HashSet<>();

        // Thread for first transaction (will modify but not commit until after read)
        Thread readerThread = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);

            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

            txTemplate.execute(status -> {

                for (int i = 0; i < 50; i++) {
                    PersonEntity person = personRepository.findAll().get(0);
                    log.info("Reading name: {}", person.getName());
                    set.add(person.getName());
                    entityManager.clear();
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // signal that we've updated but not yet committed
                log.info("Reader thread finished reading, set size: {}", set.size());
                return null;
            });
        });

        readerThread.start();

        for (int i = 0; i < NUM_THREADS; i++) {
            updateThreads[i].start();
        }

        // Wait for both threads to complete
        readerThread.join();
        for (Thread thread : updateThreads) {
            thread.join();
        }
        assertEquals(1, set.size(), "Should read consistent data");
    }


    @Test
    public void testReadUncommitted_shouldReadDirtyData_MultipleThreads() throws InterruptedException {
        final int NUM_UPDATE_THREADS = 5;
        CountDownLatch updatesStarted = new CountDownLatch(NUM_UPDATE_THREADS);
        CountDownLatch readerReady = new CountDownLatch(1);
        CountDownLatch testCompleted = new CountDownLatch(1);

        Set<String> observedValues = Collections.synchronizedSet(new HashSet<>());

        // Start reader thread first with READ_UNCOMMITTED
        Thread readerThread = new Thread(() -> {
            try {
                // Signal we're ready to start reading
                readerReady.countDown();

                // Perform multiple reads while updates are happening
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);

                // Wait for updates to start
                updatesStarted.await();

                // Read multiple times until test signals completion
                while (testCompleted.getCount() > 0) {
                    txTemplate.execute(status -> {
                        PersonEntity person = personRepository.findAll().get(0);
                        String name = person.getName();
                        observedValues.add(name);
                        entityManager.clear();
                        log.info("Reader observed: {}", name);
                        return null;
                    });
                    Thread.sleep(50); // Small delay between reads
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        readerThread.start();
        readerReady.await(); // Wait for reader to be ready

        // Start update threads
        Thread[] updateThreads = new Thread[NUM_UPDATE_THREADS];
        for (int i = 0; i < NUM_UPDATE_THREADS; i++) {
            final String newValue = "modified-by-thread-" + i;
            updateThreads[i] = new Thread(() -> {
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    try {

                        updatesStarted.countDown();

                        long sleepTime = (long) (Math.random() * 2000);
                        Thread.sleep(sleepTime);

                        PersonEntity person = personRepository.findAll().get(0);
                        person.setName(newValue);
                        personRepository.saveAndFlush(person);

                        // Hold the transaction open for a while without committing


                        return null;
                    } catch (InterruptedException e) {
                        status.setRollbackOnly();
                        Thread.currentThread().interrupt();
                        return null;
                    }
                });
            });
            updateThreads[i].start();
        }

        // Let updates and reads run for a bit
        Thread.sleep(10000);
        testCompleted.countDown();

        // Wait for all threads to complete
        for (Thread thread : updateThreads) {
            thread.join();
        }
        readerThread.join();

        // Since we're using READ_UNCOMMITTED, we expect to see dirty reads
        assertTrue(observedValues.size() > 1,
                   "With READ_UNCOMMITTED, should have observed multiple values including uncommitted ones");
        log.info("Total unique values observed: {}", observedValues.size());
        log.info("Values observed: {}", observedValues);
    }

    @Test
    public void testReadCommitted_shouldReadComitted_MultipleThreads() throws InterruptedException {
        final int NUM_UPDATE_THREADS = 5;
        CountDownLatch updatesStarted = new CountDownLatch(NUM_UPDATE_THREADS);
        CountDownLatch readerReady = new CountDownLatch(1);
        CountDownLatch testCompleted = new CountDownLatch(1);

        Set<String> observedValues = Collections.synchronizedSet(new HashSet<>());

        // Start reader thread first with READ_UNCOMMITTED
        Thread readerThread = new Thread(() -> {
            try {
                // Signal we're ready to start reading
                readerReady.countDown();

                // Perform multiple reads while updates are happening
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

                // Wait for updates to start
                updatesStarted.await();

                // Read multiple times until test signals completion
                while (testCompleted.getCount() > 0) {
                    txTemplate.execute(status -> {
                        PersonEntity person = personRepository.findAll().get(0);
                        String name = person.getName();
                        observedValues.add(name);
                        entityManager.clear();
                        log.info("Reader observed: {}", name);
                        return null;
                    });
                    Thread.sleep(50); // Small delay between reads
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        readerThread.start();
        readerReady.await(); // Wait for reader to be ready

        // Start update threads
        Thread[] updateThreads = new Thread[NUM_UPDATE_THREADS];
        for (int i = 0; i < NUM_UPDATE_THREADS; i++) {
            final String newValue = "modified-by-thread-" + i;
            updateThreads[i] = new Thread(() -> {
                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    try {

                        updatesStarted.countDown();

                        long sleepTime = (long) (Math.random() * 2000);
                        Thread.sleep(sleepTime);

                        PersonEntity person = personRepository.findAll().get(0);
                        person.setName(newValue);
                        personRepository.saveAndFlush(person);

                        // Hold the transaction open for a while without committing


                        return null;
                    } catch (InterruptedException e) {
                        status.setRollbackOnly();
                        Thread.currentThread().interrupt();
                        return null;
                    }
                });
            });
            updateThreads[i].start();
        }

        // Let updates and reads run for a bit
        Thread.sleep(10000);
        testCompleted.countDown();

        // Wait for all threads to complete
        for (Thread thread : updateThreads) {
            thread.join();
        }
        readerThread.join();

        // Since we're using READ_UNCOMMITTED, we expect to see dirty reads
        assertTrue(observedValues.size() > 1,
                   "With READ_UNCOMMITTED, should have observed multiple values including uncommitted ones");
        log.info("Total unique values observed: {}", observedValues.size());
        log.info("Values observed: {}", observedValues);
    }
}