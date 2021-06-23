package bhu.tx;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@Configuration
public class Conf {
    @Bean
    public TransactionalOperator txo(ReactiveMongoDatabaseFactory factory) {
        return TransactionalOperator.create(new ReactiveMongoTransactionManager(factory), new DefaultTransactionDefinition());
    }
}
