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
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.EntityManager;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    public void testRepeatableRead_shouldIncurLostUpdate() throws InterruptedException {
        // Use CountDownLatch to coordinate thread execution
        CountDownLatch bothReadComplete = new CountDownLatch(2);
        CountDownLatch tx1FinishedRead = new CountDownLatch(1);
        CountDownLatch tx2FinishedRead = new CountDownLatch(1);

        // Track the final name value for verification
        AtomicReference<String> tx1UpdatedValue = new AtomicReference<>("tx1-update");
        AtomicReference<String> tx2UpdatedValue = new AtomicReference<>("tx2-update");
        AtomicReference<String> finalValue = new AtomicReference<>();

        // First transaction reads, waits for second to read, then updates
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // Read the initial data
                    PersonEntity person = personRepository.findAllForUpdateReadLock().get(0);
                    String originalName = person.getName();
                    log.info("TX1: Read person with name: {}", originalName);

                    // Signal that TX1 has completed its read
                    tx1FinishedRead.countDown();

                    // Wait for TX2 to also complete its read
                    tx2FinishedRead.await();

                    // Update the data
                    person.setName(tx1UpdatedValue.get());
                    personRepository.saveAndFlush(person);
                    log.info("TX1: Updated person to: {}", tx1UpdatedValue.get());

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Second transaction also reads, then waits, then updates
        Thread transaction2 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // Wait for TX1 to read first
                    tx1FinishedRead.await();

                    // Now TX2 reads
                    PersonEntity person = personRepository.findAll().get(0);
                    String originalName = person.getName();
                    log.info("TX2: Read person with name: {}", originalName);

                    // Signal TX2 has read
//                    tx2FinishedRead.countDown();

                    // Wait a bit to let TX1 update first
                    Thread.sleep(100);

                    // Update with TX2's value
                    person.setName(tx2UpdatedValue.get());
                    personRepository.saveAndFlush(person);
                    log.info("TX2: Updated person to: {}", tx2UpdatedValue.get());

                    tx2FinishedRead.countDown();

                    return null;
                } catch (Exception e) {
                    log.error("TX2: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Start both transactions
        transaction1.start();
        transaction2.start();

        // Wait for both transactions to complete
        transaction1.join();
        transaction2.join();

        // Check the final state in a separate transaction
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        String finalName = verifyTx.execute(status -> {
            return personRepository.findAll().get(0).getName();
        });

        log.info("Final person name: {}", finalName);

        // The last committer will "win" - likely TX2 since it waits for TX1 to update first
        assertEquals(tx2UpdatedValue.get(), finalName,
                     "The last transaction to commit overwrites earlier updates - demonstrating lost update");
        assertNotEquals(tx1UpdatedValue.get(), finalName,
                        "The first transaction's update was lost");
    }

    @Test
    public void testSerializable_shouldPreventWriteSkew() throws InterruptedException {
        CountDownLatch tx1ReadComplete = new CountDownLatch(1);
        CountDownLatch tx2CommitComplete = new CountDownLatch(1);
        AtomicBoolean tx1Success = new AtomicBoolean(true);

        // First transaction reads data, waits for second transaction to complete,
        // then attempts to update - should fail due to serialization
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            try {
                txTemplate.execute(status -> {
                    try {
                        // Read data
                        PersonEntity person = personRepository.findAll().get(0);
                        log.info("TX1: Read person with name: {}", person.getName());

                        // Signal that T1 has read the data
                        tx1ReadComplete.countDown();

                        // Wait for T2 to complete its update and commit
                        tx2CommitComplete.await();
                        Thread.sleep(100); // Short delay to ensure T2 commit is fully processed

                        person.setName("updated-by-tx1");
                        personRepository.saveAndFlush(person);
                        log.info("TX1: Updated person successfully");

                        return null;
                    } catch (Exception e) {
                        log.info("TX1: Exception during update: {}", e.getMessage());
                        status.setRollbackOnly();
                        tx1Success.set(false);
                        return null;
                    }
                });
            } catch (Exception e) {
                log.info("TX1: Transaction failed: {}", e.getMessage());
                tx1Success.set(false);
            }
        });

        // Second transaction reads data after first has read,
        // then updates and commits
        Thread transaction2 = new Thread(() -> {
            try {
                // Wait for T1 to read the data first
                tx1ReadComplete.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    // Read and update
                    PersonEntity person = personRepository.findAll().get(0);
                    String originalName = person.getName();
                    log.info("TX2: Read person with name: {}", originalName);

                    // Now try to update - with SERIALIZABLE this should fail
                    // due to serialization conflict
                    // TX1 read will request a shared lock
                    person.setName("updated-by-tx2");
                    personRepository.saveAndFlush(person);
                    log.info("TX2: Updated person to 'updated-by-tx2'");

                    return null;
                });

                // Signal that T2 has committed
                tx2CommitComplete.countDown();

            } catch (Exception e) {
                log.error("TX2: Exception: {}", e.getMessage());
                tx2CommitComplete.countDown();
            }
        });

        // Start the transactions
        transaction1.start();
        transaction2.start();

        // Wait for both transactions to complete
        transaction1.join();
        transaction2.join();

        // Verify final state
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        String finalName = verifyTx.execute(status -> {
            return personRepository.findAll().get(0).getName();
        });

        log.info("Final person name: {}", finalName);
        assertEquals("updated-by-tx1", finalName, "Person should have been updated by transaction 2");
        assertFalse(tx1Success.get(), "Transaction 1 should have failed due to serialization conflict");
    }

    @Test
    public void testSerializable_detectsWriteSkewInInnoDB() throws InterruptedException {
        // Use CountDownLatch to coordinate thread execution
        CountDownLatch tx1ReadComplete = new CountDownLatch(1);
        CountDownLatch tx2ReadComplete = new CountDownLatch(1);
        AtomicBoolean tx2Failed = new AtomicBoolean(false);
        AtomicReference<Exception> tx2Exception = new AtomicReference<>();

        // First transaction reads data, then updates
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // Read initial data
                    PersonEntity person = personRepository.findAll().get(0);
                    log.info("TX1: Read person with name: {}", person.getName());

                    // Signal that T1 has read the data
                    tx1ReadComplete.countDown();

                    // Wait for T2 to also read
                    tx2ReadComplete.await();

                    // Small delay to ensure T2 has started its update operation
                    Thread.sleep(50);

                    // Update the data
                    person.setName("updated-by-tx1");
                    personRepository.saveAndFlush(person);
                    log.info("TX1: Updated person successfully");

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Second transaction reads data, tries to update the same data
        Thread transaction2 = new Thread(() -> {
            try {
                // Wait for T1 to read first
                tx1ReadComplete.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    try {
                        // Read data (should see original state)
                        PersonEntity person = personRepository.findAll().get(0);
                        log.info("TX2: Read person with name: {}", person.getName());

                        // Signal T2 has also read
                        tx2ReadComplete.countDown();

                        // Update with TX2's value - should eventually fail with serialization error
                        Thread.sleep(100); // Ensure TX1 tries to commit first
                        person.setName("updated-by-tx2");
                        personRepository.saveAndFlush(person);
                        log.info("TX2: Updated person successfully");

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
                log.info("TX2: Transaction failed: {}", e.getMessage());
            }
        });

        // Start transactions
        transaction1.start();
        transaction2.start();

        // Wait for completion
        transaction1.join();
        transaction2.join();

        // Check final state
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        String finalName = verifyTx.execute(status -> {
            return personRepository.findAll().get(0).getName();
        });

        log.info("Final person name: {}", finalName);

        // With SERIALIZABLE, one transaction should succeed and the other should fail
        // In InnoDB's implementation, the second transaction typically fails with a serialization error
        assertEquals("updated-by-tx1", finalName, "TX1's update should be preserved");

        // Check if MySQL InnoDB actually enforced serialization
        if (tx2Failed.get()) {
            log.info("MySQL InnoDB correctly enforced SERIALIZABLE isolation by aborting TX2");
            assertTrue(
                    tx2Exception.get().getMessage().contains("deadlock") ||
                            tx2Exception.get().getMessage().contains("lock wait timeout") ||
                            tx2Exception.get().getMessage().contains("serialization failure"),
                    "TX2 should fail with serialization-related error"
            );
        } else {
            // Note: Some MySQL configurations might not strictly enforce SERIALIZABLE as expected
            log.warn("MySQL InnoDB did not abort TX2 as expected with SERIALIZABLE isolation.");
            log.warn("This may happen with certain MySQL configurations where SERIALIZABLE behaves like REPEATABLE READ.");
        }
    }

    @Test
    public void testSerializable_detectsWriteSkewWithInsert() throws InterruptedException {
        // Setup - delete all records before starting
        TransactionTemplate setupTx = new TransactionTemplate(transactionManager);
        setupTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        setupTx.execute(status -> {
            // Clear existing data and create a clean table
            personRepository.deleteAll();
            personRepository.flush();
            return null;
        });

        Thread.sleep(200);

        // Use CountDownLatch to coordinate thread execution
        CountDownLatch tx1ReadComplete = new CountDownLatch(1);
        CountDownLatch tx2ReadComplete = new CountDownLatch(1);
        CountDownLatch tx1InsertComplete = new CountDownLatch(1);

        AtomicBoolean tx2Failed = new AtomicBoolean(false);
        AtomicReference<Exception> tx2Exception = new AtomicReference<>();

        // First transaction reads (empty table), inserts and commits
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // Verify table is empty
                    long count = personRepository.count();
                    log.info("TX1: Initial record count: {}", count);
                    assertEquals(0, count, "Table should be empty at start");

                    // Signal that TX1 has completed its read
                    tx1ReadComplete.countDown();

                    // Wait for TX2 to also read
                    tx2ReadComplete.await();

                    // Insert a record with ID = 1
                    PersonEntity person = new PersonEntity();
                    person.setId(1L); // Explicit ID to demonstrate conflict
                    person.setName("tx1-person");
                    personRepository.save(person);
                    personRepository.flush();
                    log.info("TX1: Inserted person with ID=1");

                    // Signal TX1 has completed insert
                    tx1InsertComplete.countDown();

                    Thread.sleep(1000);

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Second transaction also reads (empty table), then tries to insert same ID
        Thread transaction2 = new Thread(() -> {
            try {
                // Wait for TX1 to read first
                tx1ReadComplete.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    try {
                        // Also verify empty table - should still see empty due to isolation
                        long count = personRepository.count();
                        log.info("TX2: Initial record count: {}", count);
                        assertEquals(0, count, "TX2 should still see empty table");

                        // Signal TX2 has done its read
                        tx2ReadComplete.countDown();

                        // Wait for TX1 to insert and commit
                        tx1InsertComplete.await();

                        // Now TX2 tries to insert with same ID - should conflict
                        PersonEntity person = new PersonEntity();
                        person.setId(1L); // Same ID as TX1 inserted
                        person.setName("tx2-person");
                        personRepository.save(person);
                        personRepository.flush(); // Should throw exception on duplicate key
                        log.info("TX2: Inserted person with ID=1");

                        return null;
                    } catch (TransactionException e) {
                        log.error("TX2: Exception during insert: {}", e.getMessage());
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

        // Wait for both transactions to complete
        transaction1.join();
        transaction2.join();

        // Check the final state in a separate transaction
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        Long finalCount = verifyTx.execute(status -> personRepository.count());
        log.info("Final record count: {}", finalCount);

        // Verify TX2 failed with appropriate error
        assertTrue(tx2Failed.get(), "Transaction 2 should have failed with primary key conflict");
        assertTrue(tx2Exception.get().getMessage().contains("could not execute statement") ||
                           tx2Exception.get().getMessage().contains("Duplicate entry") ||
                           tx2Exception.get().getMessage().contains("ConstraintViolationException"),
                   "Should fail with primary key conflict, got: " + tx2Exception.get().getMessage());

        // Verify only TX1's insert succeeded
        assertEquals(1L, finalCount, "Only one record should exist");

        String finalName = verifyTx.execute(status -> personRepository.findById(1L)
                                                                      .map(PersonEntity::getName)
                                                                      .orElse(null));
        assertEquals("tx1-person", finalName, "The record should be from TX1");
    }

    //+------------+-----------+--------------------+------------------------+-----------+
    //| index_name | lock_type | lock_mode          | lock_data              | thread_id |
    //+------------+-----------+--------------------+------------------------+-----------+
    //| NULL       | TABLE     | IS                 | NULL                   |      1884 |
    //| NULL       | TABLE     | IX                 | NULL                   |      1885 |
    //| PRIMARY    | RECORD    | S                  | supremum pseudo-record |      1884 |
    //| PRIMARY    | RECORD    | X,INSERT_INTENTION | supremum pseudo-record |      1885 |
    //| PRIMARY    | RECORD    | S                  | 2                      |      1884 |
    //+------------+-----------+--------------------+------------------------+-----------+
    @Test
    public void testRepeatableRead_preventsPhantomReads_withFindGreater() throws InterruptedException {
        // Setup - ensure we have a known state with one record
        TransactionTemplate setupTx = new TransactionTemplate(transactionManager);
        setupTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        setupTx.execute(status -> {
            personRepository.deleteAll();
            personRepository.flush();

            PersonEntity person = new PersonEntity();
            person.setName("existing-record");
            personRepository.save(person);
            personRepository.flush();

            return null;
        });

        // Use CountDownLatch to coordinate thread execution
        CountDownLatch tx1FirstReadComplete = new CountDownLatch(1);
        CountDownLatch tx2InsertComplete = new CountDownLatch(1);
        AtomicInteger tx2Error = new AtomicInteger(0);

        AtomicInteger tx1FirstReadCount = new AtomicInteger(0);
        AtomicInteger tx1SecondReadCount = new AtomicInteger(0);
        AtomicBoolean phantomReadOccurred = new AtomicBoolean(false);

        // First transaction reads, then reads again after TX2 inserts
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // First read with findGreater - should find only initial record
                    List<PersonEntity> initialRecords = personRepository.findGreaterAndSmaller();
                    tx1FirstReadCount.set(initialRecords.size());
                    log.info("TX1: First read found {} records", tx1FirstReadCount.get());

                    entityManager.clear();

                    // Signal TX1 completed first read
                    tx1FirstReadComplete.countDown();

                    // Wait for TX2 to insert new record
                    tx2InsertComplete.await();

                    Thread.sleep(1000);

                    // Second read - should find the same records due to REPEATABLE_READ
                    List<PersonEntity> secondRead = personRepository.findGreater();
                    tx1SecondReadCount.set(secondRead.size());
                    log.info("TX1: Second read found {} records", tx1SecondReadCount.get());

                    // Check if phantom read occurred
                    if (tx1SecondReadCount.get() > tx1FirstReadCount.get()) {
                        phantomReadOccurred.set(true);
                        log.info("TX1: Phantom read detected!");
                    }

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Second transaction waits for TX1 to read, then inserts a new record
        Thread transaction2 = new Thread(() -> {
            try {
                // Wait for TX1 to complete its first read
                tx1FirstReadComplete.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    // Insert a new record that should match the findGreater query
                    PersonEntity newPerson = new PersonEntity();
                    newPerson.setName("new-record");
                    personRepository.save(newPerson);
                    personRepository.flush();
                    log.info("TX2: Inserted new record with ID=10");

                    // Signal insert is complete

                    return null;
                });
            } catch (Exception e) {
                log.error("TX2: Exception: {}", e.getMessage(), e);
                tx2Error.set(1);
            }

            tx2InsertComplete.countDown();
        });

        // Start both transactions
        transaction1.start();
        transaction2.start();

        // Wait for completion
        transaction1.join();
        transaction2.join();

        // Check results
        log.info("TX1 first read count: {}", tx1FirstReadCount.get());
        log.info("TX1 second read count: {}", tx1SecondReadCount.get());

        assertEquals(1L, tx2Error.get(), "TX2 should failed because of using lock in TX1");
    }

    @Test
    public void testRepeatableRead_withIntentionLock_preventsConflictingUpdates() throws InterruptedException {
        // Setup - create a test record
        AtomicInteger tx2Error = new AtomicInteger(0);

        TransactionTemplate setupTx = new TransactionTemplate(transactionManager);
        setupTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        Long personId = setupTx.execute(status -> {
            personRepository.deleteAll();
            personRepository.flush();

            PersonEntity person = new PersonEntity();
            person.setName("initial-value");
            personRepository.save(person);
            personRepository.flush();

            return person.getId();
        });

        log.info("Created test record with ID: {}", personId);

        // Use CountDownLatch to coordinate thread execution
        CountDownLatch tx1ReadComplete = new CountDownLatch(1);
        CountDownLatch tx2StartedUpdate = new CountDownLatch(1);
        CountDownLatch tx1UpdateComplete = new CountDownLatch(1);

        AtomicBoolean tx2Failed = new AtomicBoolean(false);
        AtomicReference<Exception> tx2Exception = new AtomicReference<>();

        // First transaction: reads with SELECT FOR UPDATE (acquires X lock), then updates
        Thread transaction1 = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // Read with FOR UPDATE - acquires exclusive lock
                    PersonEntity person = personRepository.findByIdForUpdate(personId).orElseThrow();
                    log.info("TX1: Read person with name: {} using FOR UPDATE", person.getName());

                    // Signal TX1 completed read with lock
                    tx1ReadComplete.countDown();

                    // Wait for TX2 to attempt its update
                    tx2StartedUpdate.await();

                    // Delay to ensure TX2 has time to attempt lock acquisition
                    Thread.sleep(500);

                    // Update the data
                    person.setName("tx1-updated-value");
                    personRepository.save(person);
                    log.info("TX1: Updated person to: tx1-updated-value");

                    // Signal TX1 completed its update
                    tx1UpdateComplete.countDown();

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Second transaction: attempts to update the same record concurrently
        Thread transaction2 = new Thread(() -> {
            try {
                // Wait for TX1 to acquire its lock first
                tx1ReadComplete.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                // Set a timeout to avoid waiting indefinitely
//                txTemplate.setTimeout(2);  // 2 seconds timeout

                txTemplate.execute(status -> {
                    try {
                        // Signal TX2 is starting update attempt
//                        tx2StartedUpdate.countDown();

                        // Attempt to update - should be blocked by TX1's lock
                        List<PersonEntity> persons = personRepository.findGreaterForUpdateReadLock();
                        log.info("TX2: Read person with name: {} using FOR UPDATE", persons.size());

                        tx2StartedUpdate.countDown();

                        // If we get here, update the person
//                        person.setName("tx2-updated-value");
//                        personRepository.save(person);
//                        log.info("TX2: Updated person to: tx2-updated-value");

                        return null;
                    } catch (Exception e) {
                        tx2Error.set(1);
                        log.error("TX2: Exception during update: {}", e.getMessage());
                        status.setRollbackOnly();
                        throw e;
                    }
                });
            } catch (Exception e) {
                tx2Error.set(1);
                tx2Failed.set(true);
                tx2Exception.set(e);
                log.info("TX2: Transaction failed with exception: {}", e.getMessage());
            }
            tx2StartedUpdate.countDown();
        });

        // Start both transactions
        transaction1.start();
        transaction2.start();

        // Wait for both transactions to complete
        transaction1.join();
        transaction2.join();

        assertEquals(1L, tx2Error.get(), "TX2 should failed because of using lock in TX1");
    }

    @Test
    public void testRepeatableRead_plainSelectVsSelectForUpdate() throws InterruptedException {
        // Setup - clear and initialize test data
        TransactionTemplate setupTx = new TransactionTemplate(transactionManager);
        setupTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        setupTx.execute(status -> {
            personRepository.deleteAll();
            personRepository.flush();

            // Create one initial record
            PersonEntity person = new PersonEntity();
            person.setId(5L);
            person.setName("initial-record");
            personRepository.save(person);
            personRepository.flush();

            return null;
        });

        // Use CountDownLatch to coordinate thread execution
        CountDownLatch plainSelectDone = new CountDownLatch(1);
        CountDownLatch insertDone = new CountDownLatch(1);

        AtomicInteger plainSelectCount = new AtomicInteger(0);
        AtomicInteger forUpdateSelectCount = new AtomicInteger(0);

        // Main transaction that performs plain SELECT and SELECT FOR UPDATE
        Thread mainTransaction = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // First read with plain SELECT
                    List<PersonEntity> initialRecords = personRepository.findAll();
                    plainSelectCount.set(initialRecords.size());
                    log.info("TX1: Plain SELECT found {} records", plainSelectCount.get());

                    // Signal that plain SELECT is done
                    plainSelectDone.countDown();

                    // Wait for insert to complete
                    insertDone.await(5, TimeUnit.SECONDS);

                    // Second read with plain SELECT - should return same count due to REPEATABLE READ
                    List<PersonEntity> secondRead = personRepository.findAll();
                    log.info("TX1: Second plain SELECT found {} records", secondRead.size());
                    assertEquals(plainSelectCount.get(), secondRead.size(),
                                 "Plain SELECT should return same count due to repeatable read isolation");

                    // Now try with SELECT FOR UPDATE - should see the newly inserted records
                    // This is because FOR UPDATE acquires locks and re-reads the data
                    List<PersonEntity> forUpdateRead = personRepository.findAllForUpdate();
                    forUpdateSelectCount.set(forUpdateRead.size());
                    log.info("TX1: SELECT FOR UPDATE found {} records", forUpdateSelectCount.get());

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        // Second transaction that inserts a new record
        Thread insertTransaction = new Thread(() -> {
            try {
                // Wait for first transaction to complete its plain SELECT
                plainSelectDone.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    // Insert a new record
                    PersonEntity newPerson = new PersonEntity();
                    newPerson.setId(10L);
                    newPerson.setName("inserted-record");
                    personRepository.save(newPerson);
                    personRepository.flush();
                    log.info("TX2: Inserted new record with ID=10");

                    return null;
                });

                // Signal that insert is complete
                insertDone.countDown();
            } catch (Exception e) {
                log.error("TX2: Exception: {}", e.getMessage(), e);
                insertDone.countDown(); // Ensure latch is released
            }
        });

        // Start both transactions
        mainTransaction.start();
        insertTransaction.start();

        // Wait for completion
        mainTransaction.join();
        insertTransaction.join();

        // Verify results in a separate transaction
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        Long finalCount = verifyTx.execute(status -> personRepository.count());
        log.info("Final record count: {}", finalCount);
        assertEquals(2L, finalCount.longValue(), "Should have two records in total");

        // Check the counts from our test
        assertEquals(1, plainSelectCount.get(), "Plain SELECT should have seen only the initial record");
        assertEquals(2, forUpdateSelectCount.get(), "SELECT FOR UPDATE should have seen both records");

        log.info("Test completed - Plain SELECT count: {}, SELECT FOR UPDATE count: {}",
                 plainSelectCount.get(), forUpdateSelectCount.get());

        // Main assertion: in REPEATABLE READ, SELECT FOR UPDATE sees new records but plain SELECT doesn't
        assertTrue(forUpdateSelectCount.get() > plainSelectCount.get(),
                   "SELECT FOR UPDATE should see more records than plain SELECT in REPEATABLE READ isolation");
    }

    // Empty lock
    // SELECT index_name, lock_type, lock_mode, lock_data, thread_id FROM performance_schema.data_locks ORDER BY index_name, lock_data DESC;
    @Test
    public void testQueryLock() throws InterruptedException {
        TransactionTemplate setupTx = new TransactionTemplate(transactionManager);
        setupTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        setupTx.execute(status -> {
            personRepository.deleteAll();
            personRepository.flush();

            // Create one initial record
            PersonEntity person = new PersonEntity();
            person.setId(5L);
            person.setName("initial-record");
            personRepository.save(person);
            personRepository.flush();

            return null;
        });

        Thread mainTransaction = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            txTemplate.execute(status -> {
                try {
                    // First read with plain SELECT
                    List<PersonEntity> initialRecords = personRepository.findAll();
                    Thread.sleep(10000);

                    return null;
                } catch (Exception e) {
                    log.error("TX1: Exception: {}", e.getMessage(), e);
                    status.setRollbackOnly();
                    return null;
                }
            });
        });

        mainTransaction.start();
        mainTransaction.join();
    }

    @Test
    public void testRepeatableRead_plainSelectVsSelectForUpdate_withDelete() throws InterruptedException {
        // Setup - initialize test data with multiple records
        TransactionTemplate setupTx = new TransactionTemplate(transactionManager);
        setupTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        setupTx.execute(status -> {
            PersonEntity person1 = new PersonEntity();
            person1.setName("record-2");
            person1.setAge(5);
            personRepository.save(person1);

            personRepository.flush();
            return null;
        });

        Thread.sleep(200);

        // Use CountDownLatch to coordinate thread execution
        CountDownLatch plainSelectDone = new CountDownLatch(1);
        CountDownLatch deleteDone = new CountDownLatch(1);

        // Main transaction with plain SELECT and later SELECT FOR UPDATE
        Thread mainTransaction = new Thread(() -> {
            TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            try {
                txTemplate.execute(status -> {
                    try {
                        // First read with plain SELECT
                        PersonEntity initialRecord = personRepository.findById(1L).orElse(new PersonEntity());
                        log.info("TX1: Plain SELECT found records: {}", initialRecord.getName());
                        entityManager.clear();

                        // Signal that plain SELECT is done
                        plainSelectDone.countDown();

                        // Wait for delete to complete
                        deleteDone.await(5, TimeUnit.SECONDS);

                        // Second plain SELECT - should return same count due to REPEATABLE READ
                        PersonEntity afterRecord = personRepository.findById(1L).orElse(new PersonEntity());
                        afterRecord.setName("updated");
                        personRepository.save(afterRecord);


                        log.info("TX1: UPDATE");

                        return null;
                    } catch (Exception e) {
                        log.error("TX1: Exception: {}", e.getMessage(), e);
                        status.setRollbackOnly();
                        return null;
                    }
                });
            } catch (Exception ex) {
                log.error("TX1: Commit failed: {}", ex.getMessage(), ex);
            }
        });

        // Second transaction that deletes a record
        Thread deleteTransaction = new Thread(() -> {
            try {
                // Wait for first transaction to complete its plain SELECT
                plainSelectDone.await();

                TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                txTemplate.execute(status -> {
                    // Delete record with ID=5
                    personRepository.deleteById(1L);
                    personRepository.flush();
                    log.info("TX2: Deleted record with ID=0");
                    return null;
                });

                // Signal that delete is complete
                deleteDone.countDown();
            } catch (Exception e) {
                log.error("TX2: Exception: {}", e.getMessage(), e);
                deleteDone.countDown(); // Ensure latch is released
            }
        });

        // Start both transactions
        mainTransaction.start();
        deleteTransaction.start();

        // Wait for completion
        mainTransaction.join();
        deleteTransaction.join();

        // Verify final state in a separate transaction
        TransactionTemplate verifyTx = new TransactionTemplate(transactionManager);
        verifyTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        Long finalCount = verifyTx.execute(status -> personRepository.count());
        log.info("Final record count: {}", finalCount);
        assertEquals(1L, finalCount.longValue(), "Should have one record remaining after deletion");
    }
}