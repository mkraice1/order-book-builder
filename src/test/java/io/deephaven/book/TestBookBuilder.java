package io.deephaven.book;

import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_INT;

import java.time.Instant;

import static io.deephaven.engine.util.TableTools.*;

public class TestBookBuilder {
    private static final String UPDATE_TIME_NAME = "UpdateTimestamp";
    private static final String ORDID_NAME = "OrderId";
    private static final String SYM_NAME = "Symbol";
    private static final String ORD_TIME_NAME = "OrderTimestamp";
    private static final String PRC_NAME = "Price";
    private static final String SIZE_NAME = "Size";
    private static final String SIDE_NAME = "Side";
    Table exSource;
    Table expectedBook;
    Table resultBook;

    @BeforeClass
    public static void setupGlobal() {
        AsyncClientErrorNotifier.setReporter(t -> {
            t.printStackTrace(System.err);
            TestCase.fail(t.getMessage());
        });
    }

    @Before
    public void setup() {
        final Instant nowTime = Instant.now();

        exSource = TableTools.newTable(
                longCol("ORD_ID", 10, 20, 10, 30, 30),
                longCol("PREV_ORD_ID", NULL_LONG, NULL_LONG, NULL_LONG, 20, NULL_LONG),
                stringCol("SYMB", "SPY", "SPY", "SPY", "SPY", "SPY"),
                instantCol("EPOCH_TS", nowTime.plusSeconds(1), nowTime.plusSeconds(2),
                        nowTime.plusSeconds(3), nowTime.plusSeconds(4), nowTime.plusSeconds(5)),
                doubleCol("PRC", 1.5, 2.5, 3.5, 5.5, 5.5),
                intCol("QTY", 15, 25, NULL_INT, 55, NULL_INT),
                intCol("EXEC_QTY", NULL_INT, NULL_INT, NULL_INT, NULL_INT, 12),
                intCol("SIDE", 1, 2, 1, 2, 2),
                intCol("EVT_ID", 1, 1, 2, 4, 3)
        ).sort("EPOCH_TS");

        expectedBook = TableTools.newTable(instantCol(UPDATE_TIME_NAME, nowTime),
                longCol(ORDID_NAME, 30),
                stringCol(SYM_NAME, "SPY"),
                instantCol(ORD_TIME_NAME, nowTime.plusSeconds(4)),
                doubleCol(PRC_NAME,  5.5),
                intCol(SIZE_NAME,  43),
                intCol(SIDE_NAME, 2)
        );
    }

    @Test
    public void testSimple() throws InterruptedException, ExecutionException {
        resultBook = PriceBook.build(exSource, "ORD_ID", "PREV_ORD_ID",
                "SYMB", "EPOCH_TS", "PRC", "QTY",
                "EXEC_QTY", "SIDE", "EVT_ID");

        // Hard to verify the exact instant something got updated...
        resultBook = resultBook.dropColumns("UpdateTimestamp");
        expectedBook = expectedBook.dropColumns("UpdateTimestamp");

        TestCase.assertEquals("", TableTools.diff(resultBook, expectedBook, 10));
    }

}
