package com.example.databaseindex;

import com.example.databaseindex.service.PersonService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootApplication
@Slf4j
public class DatabaseIndexApplication implements ApplicationContextAware {
    
    private static ApplicationContext applicationContext;
    private static PersonService personService;
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        DatabaseIndexApplication.applicationContext = applicationContext;
    }
    
    public static void main(String[] args) throws InterruptedException, BrokenBarrierException, TimeoutException {
        SpringApplication.run(DatabaseIndexApplication.class, args);
        personService = (PersonService) applicationContext.getBean("personService");
        personService.init();
        testReadCommittedRow();
        testReadCommitWithPhantomPhenomenon();
        testReadCommitWithoutPhantomPhenomenon();
    }
    
    private static void testReadCommittedRow() throws InterruptedException, BrokenBarrierException, TimeoutException {
        var cyclicBarrier = new CyclicBarrier(2);
        var t1 = new Thread(() -> {
            var readBeforeAndAfter = personService.readCommittedPerson(1L);
            log.info("Username before is: {}", readBeforeAndAfter.getLeft().getName());
            log.info("Expect username after read row committed by another transaction: dnt updated, actual: {}", readBeforeAndAfter.getRight().getName());
            assertEquals("dnt updated", readBeforeAndAfter.getRight().getName());
            try {
                cyclicBarrier.await(10L, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
        var t2 = new Thread(() -> {
            personService.updatePerson(1L);
        });
        t1.start();
        t2.start();
        cyclicBarrier.await(10L, TimeUnit.SECONDS);
    }
    
    static void testReadCommitWithPhantomPhenomenon() throws BrokenBarrierException, InterruptedException, TimeoutException {
        var cyclicBarrier = new CyclicBarrier(2);
        var t1 = new Thread(() -> {
            var readBeforeAndAfter = personService.readPersons();
            log.info("Number of rows before: {}", readBeforeAndAfter.getLeft().size());
            log.info("Expect number of row increase 1, expected: 3, actual: {}", readBeforeAndAfter.getLeft().size());
            try {
                cyclicBarrier.await(10L, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
        t1.start();
        personService.addPerson();
        cyclicBarrier.await(10L, TimeUnit.SECONDS);
    }
    
    static void testReadCommitWithoutPhantomPhenomenon() throws BrokenBarrierException, InterruptedException, TimeoutException {
        var cyclicBarrier = new CyclicBarrier(2);
        var t1 = new Thread(() -> {
            var readBeforeAndAfter = personService.readPersonsWithRepeatableRead();
            log.info("Number of rows before (3): {}", readBeforeAndAfter.getLeft().size());
            log.info("Expect number of row don't increase 1, expected: 3, actual: {}", readBeforeAndAfter.getRight().size());
            try {
                cyclicBarrier.await(10L, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
        t1.start();
        personService.addPerson();
        cyclicBarrier.await(10L, TimeUnit.SECONDS);
    }
    
}
