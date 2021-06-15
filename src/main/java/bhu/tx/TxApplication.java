package bhu.tx;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.springframework.data.mongodb.core.query.Criteria.where;

@SpringBootApplication
public class TxApplication implements CommandLineRunner {
    Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        SpringApplication.run(TxApplication.class, args);
    }

    @Autowired
    ReactiveMongoTemplate mongo;
    @Autowired
    ReactiveMongoDatabaseFactory factory;

    @Override
    public void run(java.lang.String... args)  {
        mongo.remove(Query.query(where("_id").exists(true)), A.class).block();
        mongo.remove(Query.query(where("_id").exists(true)), B.class).block();

        mongo.setSessionSynchronization(SessionSynchronization.ALWAYS);
        var txo = TransactionalOperator.create(new ReactiveMongoTransactionManager(factory), new DefaultTransactionDefinition());

        Flux.range(0, 1000)
                .flatMap(i ->
                    txo.transactional(mongo.save(new A(i))
                            .flatMap(it -> i % 30 == 29 ? Mono.error(new Exception("")) : Mono.empty())
                            .concatWith(mongo.save(new B(i))))
                )
                .subscribe();
    }
}

class A {
    public String id = ObjectId.get().toHexString();
    public Integer i;

    public A(Integer i) {
        this.i = i;
    }
}


class B {
    public String id = ObjectId.get().toHexString();
    public Integer i;

    public B(Integer i) {
        this.i = i;
    }
}