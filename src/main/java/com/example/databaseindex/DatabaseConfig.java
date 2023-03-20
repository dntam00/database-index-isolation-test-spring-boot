package com.example.databaseindex;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@Profile("db")
@PropertySource("classpath:application.properties")
@EnableTransactionManagement
@ComponentScan
public class DatabaseConfig {
    
    public static final String JPA_TRANSACTION_TEMPLATE_BEAN_NAME = "jpaTransactionTemplate";
    
    /**
     * Force spring context to have a transaction manager for UT and IT
     * */
    @Primary
    @Bean("transactionManager")
    public JpaTransactionManager jpaTransactionManager(LocalContainerEntityManagerFactoryBean dbEntityManager) {
        JpaTransactionManager transactionManager
                = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(
                dbEntityManager.getObject());
        return transactionManager;
    }
    
    @Bean(JPA_TRANSACTION_TEMPLATE_BEAN_NAME)
    public TransactionTemplate jpaTransactionTemplate(JpaTransactionManager jpaTransactionManager) {
        return new TransactionTemplate(jpaTransactionManager);
    }
}
