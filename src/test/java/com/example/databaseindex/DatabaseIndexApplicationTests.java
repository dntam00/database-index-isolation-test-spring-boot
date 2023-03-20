package com.example.databaseindex;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        DatabaseConfig.class
})
@EnableTransactionManagement
@Transactional
@ActiveProfiles("db")
class DatabaseIndexApplicationTests {
    

}
