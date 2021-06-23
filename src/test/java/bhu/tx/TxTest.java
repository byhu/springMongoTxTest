package bhu.tx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@SpringBootTest
@TestPropertySource("classpath:application.properties")
public class TxTest {
    @Autowired
    ReactiveMongoTemplate mongo;
    @Autowired
    TransactionalOperator txo;

    private void cleanup(String col) {
        mongo.findAllAndRemove(query(where("id").exists(true)), A.class, col + "0")
                .then(mongo.remove(query(where("id").exists(true)), A.class, col + "1"))
                .block();
    }

    Mono<Void> save(int i, String col) {
        var d0 = new A(i, "a1 " + i);
        var d1 = new A(i, "a2 " + i);
        return mongo.save(d0, col + "0")
                .flatMap(it -> {
                    if (i % 30 == 29)
                        return Mono.error(new Exception("Out"));
                    return Mono.empty();
                })
                .then(mongo.save(d1, col + "1"))
                .then();
    }

    private Mono<Void> countEqual(String col) {
        return Mono.zip(
                mongo.count(query(new Criteria()), A.class, col + "0"),
                mongo.count(query(new Criteria()), A.class, col + "1")
        ).doOnNext(cs -> {
            System.out.println(col + " counts: " + cs);
            Assertions.assertEquals(cs.getT1(), cs.getT2());
        }).then();
    }

    @Test
    public void flux() {
        cleanup("flux");
        try {
            Flux.range(0, 100)
                    .flatMap(i -> txo.transactional(save(i, "flux")))
//                    .then()
//                .onErrorResume(e -> Mono.empty())
                    .then(countEqual("flux")).block();
        } catch (Exception e) {
            Mono.delay(Duration.ofSeconds(2))
                    .then(countEqual("flux"))
                    .block();
        }
    }

    @Test
    public void fluxSingleTx() {
        cleanup("fluxSingleTx");
        try {
            txo.transactional(Flux.range(0, 100)
                    .flatMap(i -> save(i, "fluxSingleTx")))
                    .then()
//                .onErrorResume(e -> Mono.empty())
                    .then(countEqual("fluxSingleTx")).block();
        } catch (Exception e) {
            countEqual("fluxSingleTx").block();
        }
    }

    @Test
    public void monoZip() {
        cleanup("monoZip");
        try {
            var l = IntStream.range(1, 100).mapToObj(i -> txo.transactional(save(i, "monoZip"))).collect(Collectors.toList());

            Mono.zip(l, a -> a)
                    .then()
//                .onErrorResume(e -> Mono.empty())
                    .then(countEqual("monoZip")).block();
        } catch (Exception e) {
            countEqual("monoZip").block();
        }
    }

    @Test
    public void mono() {
        cleanup("mono");
        try {
            var l =
                    IntStream.range(1, 100).mapToObj(i -> txo.transactional(save(i, "mono"))).collect(Collectors.toList());

            l.stream().reduce(Mono::then)
                    .get()
                    .then()
//                .onErrorResume(e -> Mono.empty())
                    .then(countEqual("mono")).block();
        } catch (Exception e) {
            countEqual("mono").block();
        }
    }

    @Test
    public void monoConcat() {
        cleanup("monoConcat");
        try {
            var m =
                    txo.transactional(save(0, "monoConcat"))
                            .concatWith(txo.transactional(save(1, "monoConcat")));

            for (int i = 2; i < 100; i++)
                m = m.concatWith(txo.transactional(save(i, "monoConcat")));

            m.then()
//                    .onErrorResume(e -> Mono.empty())
                    .then(countEqual("monoConcat")).block();
        } catch (Exception e) {
            countEqual("monoConcat").block();
        }
    }

    Mono<Void> recursive(int i) {
        if (i > 100)
            return Mono.empty();
        return txo.transactional(save(i, "recursive"))
                .then(recursive(i + 1));
    }

    @Test
    public void recursive() {
        cleanup("recursive");
        try {
            recursive(100)
//                    .onErrorResume(e -> Mono.empty())
                    .then(countEqual("recursive")).block();
        } catch (Exception e) {
            countEqual("recursive").block();
        }
    }
}

class A {
    public int id;

    public A(int id, String data) {
        this.id = id;
        this.data = data;
    }

    public String data;
}
