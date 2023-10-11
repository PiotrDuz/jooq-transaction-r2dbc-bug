package org.test;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.SQLDataType.VARCHAR;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class JooqReactiveTest {

    public static final String TAB = "tab";
    public static final String COL_1 = "col1";
    public static final String VALUE = "text1";
    // will be shared between test methods
    @Container
    private final PostgreSQLContainer<?> CONTAINER = new PostgreSQLContainer<>("postgres:14");

    private DSLContext ctx;

    @BeforeEach
    public void setupSpec() {
        assertTrue(CONTAINER.isRunning());
        ConnectionFactory pooledConnectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "pool")
                .option(PROTOCOL, "postgresql") // driver identifier, PROTOCOL is delegated as DRIVER by the pool.
                .option(HOST, CONTAINER.getHost())
                .option(PORT, CONTAINER.getFirstMappedPort())
                .option(USER, CONTAINER.getUsername())
                .option(PASSWORD, CONTAINER.getPassword())
                .option(DATABASE, CONTAINER.getDatabaseName())
                .build());
        DSLContext ctx = DSL.using(pooledConnectionFactory);
        Flux.from(ctx.createTable(TAB).column(COL_1, VARCHAR)).then().block();
        Flux.from(ctx.insertInto(table(TAB), field(COL_1)).values(VALUE)).then().block();
        this.ctx = ctx;
    }

    @Test
    @Timeout(5)
    void testWithExceptionThrownOnFlowAssemblyInsideTransactionBlock() {
        Object text = Mono.from(ctx.transactionPublisher(trans -> {
                    return Mono.just(throwExc()).then(getFrom(trans));
                }))
                .block();
        assertEquals(VALUE, text);
    }

    @Test
    void testWithProperFlowAssembled() {
        Object text = Mono.from(ctx.transactionPublisher(trans -> {
                    return getFrom(trans);
                }))
                .block();
        assertEquals(VALUE, text);
    }


    @Test
    void testWithExceptionContainedInsideReactiveFlow() {
        Mono flow = Mono.from(ctx.transactionPublisher(trans -> {
            return Mono.defer(() -> Mono.just(throwExc()).then(getFrom(trans)));
        }));
        assertThrows(IllegalStateException.class, () -> flow.block());
    }


    private Mono<Object> getFrom(Configuration trans) {
        return Flux.from(trans.dsl().select(field(COL_1)).from(table(TAB))).map(rec -> rec.value1()).single();
    }

    private Object throwExc() {
        throw new IllegalStateException();
    }
}
