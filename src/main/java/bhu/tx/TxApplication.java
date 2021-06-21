package bhu.tx;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class TxApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(TxApplication.class, args);
    }

    @Autowired
    ReactiveMongoTemplate mongo;
    //    @Autowired
//    MongoTemplate imongo;
    @Autowired
    ReactiveMongoDatabaseFactory factory;

    @java.lang.Override
    public void run(java.lang.String... args) throws Exception {
        mongo.remove(A1.class).all().block();
        mongo.remove(A2.class).all().block();

        // session synchronization doesn't seem to make any difference
        mongo.setSessionSynchronization(SessionSynchronization.ALWAYS);

        var txo = TransactionalOperator.create(new ReactiveMongoTransactionManager(factory), new DefaultTransactionDefinition());

        // test cases
        flux(txo);      // failed
//        monoZip(txo); // failed
//        fluxO(txo);   // failed, if you run multiple times, you will see 1 or 2 records of A1 inserted.

//        recursive(txo, 0).block(); // success
//        mono(txo);        // success
//        monoConcat(txo);  // success
    }

    private Mono<A2> save(int i) {
        var d0 = new A1(i, "asdf " + i);
        var d1 = new A2(i, "asdfadf " + i);
        return mongo.save(d0)
                .flatMap(it -> {
                    if (i % 20 == 19)
                        return Mono.error(new Exception("ddd"));
                    return Mono.empty();
                })
                .then(mongo.save(d1));
    }

    private Mono<Void> recursive(TransactionalOperator txo, int i) {
        if (i > 1000)
            return Mono.empty();
        return txo.transactional(save(i))
                .onErrorResume(e -> Mono.empty())
                .then(recursive(txo, i + 1));
    }

    private void flux(TransactionalOperator txo) {
        Flux.range(0, 1000)
                .flatMap(i -> txo.transactional(save(i)))
                .onErrorResume(e -> Mono.empty())
                .blockLast();
    }

    private void fluxO(TransactionalOperator txo) {
        txo.transactional(Flux.range(0, 1000)
                .flatMap(this::save))
                .blockLast();
    }

    private void mono(TransactionalOperator txo) {
        var l = IntStream.range(1, 1000).mapToObj(i -> txo.transactional(save(i))).collect(Collectors.toList());
        l.stream().reduce((a, b) -> a.then(b))
                .get()
                .block();
    }

    private void monoConcat(TransactionalOperator txo) {
        var  m = txo.transactional(save(0)).concatWith(txo.transactional(save(1)));
        for (int i = 2; i < 100; i++)
            m = m.concatWith(txo.transactional(save(i)));
        m.onErrorContinue((e, a) -> {})
                .blockLast();
    }

    private void monoZip(TransactionalOperator txo) {
        var l = IntStream.range(1, 1000).mapToObj(i -> txo.transactional(save(i))).collect(Collectors.toList());
        Mono.zip(l, a -> a)
                .onErrorResume(e -> Mono.empty())
                .block();
        System.exit(0);
    }
}

class A1 {
    public int id;

    public A1() {
    }

    public A1(int id, String data) {
        this.id = id;
        this.data = data;
    }

    public String data;
}

class A2 {
    public int id;

    public A2() {
    }

    public A2(int id, String data) {
        this.id = id;
        this.data = data;
    }

    public String data;
}