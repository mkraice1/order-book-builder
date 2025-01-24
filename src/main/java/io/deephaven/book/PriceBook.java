package io.deephaven.book;

import java.lang.String;
import java.time.Instant;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.jetbrains.annotations.NotNull;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;

import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * <p>Build a book of current live orders</p>
 * <p>
 *     Creating a price book is as simple as invoking the static build method, specifying the column names of
 *     your source table.
 *
 *      <pre>{@code
 *      import io.deephaven.book.PriceBook
 *
 *      orderStream = db.t("Market", "Orders")
 *                      .where("Date=`2021-01-13`", "Time > '2021-01-13T08:30:00 NY'")
 *                      .sort("Time")
 *
 *      book = PriceBook.build(orderStream,"ORD_ID", "PREV_ORD_ID", "SYMB", "EPOCH_TS",  "PRC",
 *                "QTY", "EXEC_QTY", "SIDE", "EVT_ID")
 *
 *      }</pre>
 *
 * <p></p>
 * <p>
 *
 */
public class PriceBook {
    private static final int CHUNK_SIZE = 2048;
    private static final int SIDE_BUY = 1;
    private static final int SIDE_SELL = 2;

    private static final int OP_OAK = 1;
    private static final int OP_CC = 2;
    private static final int OP_INF = 3;
    private static final int OP_CRAK = 4;

    // Names of the output table columns
    private static final String UPDATE_TIME_NAME = "UpdateTimestamp";
    private static final String ORDID_NAME = "OrderId";
    private static final String SYM_NAME = "Symbol";
    private static final String ORD_TIME_NAME = "OrderTimestamp";
    private static final String PRC_NAME = "Price";
    private static final String SIZE_NAME = "Size";
    private static final String SIDE_NAME = "Side";


    // region Input Sources
    private final ColumnSource<Long> ordIdSource;
    private final ColumnSource<Long> prevOrderIdSource;
    private final ColumnSource<Long> orderTimeSource;
    private final ColumnSource<Double> priceSource;
    private final ColumnSource<Integer> sizeSource;
    private final ColumnSource<Integer> execSizeSource;
    private final ColumnSource<Integer> sideSource;
    private final ColumnSource<Integer> opSource;
    private final ColumnSource<String> symSource;
    // endregion

    // region OutputSources
    final QueryTable resultTable;
    final BookListener bookListener;

    final TrackingWritableRowSet resultIndex;

    final InstantArraySource updateTimeResults;
    final DoubleArraySource priceResults;
    final InstantArraySource orderTimeResults;
    final IntegerArraySource sizeResults;
    final IntegerArraySource sideResults;
    final LongArraySource ordIdResults;
    final ObjectArraySource<String> symResults;
    // endregion

    // region Book state objects
    private final Long2LongOpenHashMap orderMap = new Long2LongOpenHashMap();
    private final LongOpenHashSet  availableRows = new LongOpenHashSet();
    private long  resultSize;
    // endregion

    private final boolean sourceIsBlink;

    private PriceBook(@NotNull final Table table,
                      @NotNull String idColumnName,
                      @NotNull String prevIdColumnName,
                      @NotNull String symColumnName,
                      @NotNull String timestampColumnName,
                      @NotNull String priceColumnName,
                      @NotNull String sizeColumnName,
                      @NotNull String execSizeColumnName,
                      @NotNull String sideColumnName,
                      @NotNull String opColumnName,
                      Table snapshot) {
        final QueryTable source = (QueryTable) table.coalesce();
        this.sourceIsBlink = BlinkTableTools.isBlink(source);
        this.orderMap.defaultReturnValue(-1);
        this.resultSize = 0;

        // Begin by getting references to the column sources from the input table to process later.
        this.ordIdSource        = source.getColumnSource(idColumnName);
        this.prevOrderIdSource  = source.getColumnSource(prevIdColumnName);
        this.symSource          = source.getColumnSource(symColumnName);
        this.orderTimeSource    = ReinterpretUtils.instantToLongSource(source.getColumnSource(timestampColumnName));
        this.priceSource        = source.getColumnSource(priceColumnName);
        this.sizeSource         = source.getColumnSource(sizeColumnName);
        this.execSizeSource     = source.getColumnSource(execSizeColumnName);
        this.sideSource         = source.getColumnSource(sideColumnName);
        this.opSource           = source.getColumnSource(opColumnName);

        // Construct the new column sources and result table.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Now we create the columns which will be in the output table.
        // Column for the update time
        updateTimeResults = new InstantArraySource();
        columnSourceMap.put(UPDATE_TIME_NAME, updateTimeResults);

        // Set output table columns
        ordIdResults        = new LongArraySource();
        symResults          = new ObjectArraySource(String.class);
        orderTimeResults    = new InstantArraySource();
        priceResults        = new DoubleArraySource();
        sizeResults         = new IntegerArraySource();
        sideResults         = new IntegerArraySource();

        columnSourceMap.put(ORDID_NAME, ordIdResults);
        columnSourceMap.put(SYM_NAME, symResults);
        columnSourceMap.put(ORD_TIME_NAME, orderTimeResults);
        columnSourceMap.put(PRC_NAME, priceResults);
        columnSourceMap.put(SIZE_NAME, sizeResults);
        columnSourceMap.put(SIDE_NAME, sideResults);

        // Finally, create the result table for the user
        final OperationSnapshotControl snapshotControl =
                source.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);

        final MutableObject<QueryTable> result = new MutableObject<>();
        final MutableObject<BookListener> listenerHolder = new MutableObject<>();
        final WritableRowSet snapAdded;


        if (snapshot != null) {
            // Process the snapshot first
            TableUpdateImpl resultUpdate = new TableUpdateImpl();
            processInitBook(snapshot, resultUpdate);
            snapAdded = (WritableRowSet) resultUpdate.added;
        } else {
            snapAdded = RowSetFactory.empty();
        }

        QueryTable.initializeWithSnapshot("bookBuilder", snapshotControl,
                (prevRequested, beforeClock) -> {
                    final boolean usePrev = prevRequested && source.isRefreshing();

                    // Initialize the internal state by processing the entire input table.  This will be done asynchronously from
                    // the LTM thread and so it must know if it should use previous values or current values.
                    // Using the same update from processInitBook if there was a snapshot
                    TableUpdateImpl resultUpdate = new TableUpdateImpl();
                    processAdded(usePrev ? source.getRowSet().prev() : source.getRowSet(), usePrev, resultUpdate);

                    WritableRowSet resultRows = (WritableRowSet) resultUpdate.added;
                    // Add rows from snapshot.
                    resultRows.insert(snapAdded);
                    resultRows.remove(resultUpdate.removed);

                    final QueryTable bookTable = new QueryTable( (resultRows).toTracking(), columnSourceMap);

                    if (snapshotControl != null) {
                        columnSourceMap.values().forEach(ColumnSource::startTrackingPrevValues);
                        bookTable.setRefreshing(true);
                        bookTable.getRowSet().writableCast().initializePreviousValue();

                        // To Produce a blink table we need to be able to respond to upstream changes when they exist
                        // and we also need to be able to produce downstream updates, even if upstream did NOT change.
                        // This is what the ListenerRecorder and MergedListener (BookListener) are for.  The
                        // ListenerRecorder will simply record any TableUpdate produced by the source and notify the
                        // MergedListener (BookListener) that something happened.
                        // The BookListener will wait for both the UpdateGraphProcessor AND the ListenerRecorder to
                        // be satisfied before it does process().  This way, we properly handle upstream ticks, and
                        // also properly blink out rows when there are no updates.
                        final ListenerRecorder recorder = new ListenerRecorder("BookBuilderListenerRecorder", source, null);
                        final BookListener bl = new BookListener(recorder, source, bookTable);
                        recorder.setMergedListener(bl);
                        bookTable.addParentReference(bl);
                        snapshotControl.setListenerAndResult(recorder, bookTable);
                        listenerHolder.set(bl);
                    }
                    result.set(bookTable);
                    return true;
                });

        this.resultTable = result.get();
        this.resultIndex = resultTable.getRowSet().writableCast();
        if(source.isRefreshing()) {
            this.bookListener = listenerHolder.get();
            bookListener.getUpdateGraph().addSource(bookListener);
        } else {
            bookListener = null;
        }
    }

    /**
     * Process all the added rows, potentially using previous values.
     *
     * @param added the index of added rows from the source
     * @param usePrev if previous values should be used
     * @param resultUpdate the update object that will be used in the listener to push out the added, removed,
     *                     and modified RowSets
     *
     */
    @SuppressWarnings("unchecked")
    private void processAdded(RowSet added, boolean usePrev, TableUpdateImpl resultUpdate) {
        // First create the context object in a try-with-resources so it gets automatically cleaned up when we're done.
        try(final Context ctx = new Context()) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = added.getRowSequenceIterator();

            // In order to copy data into the writable chunks in the context we need to create
            // a fill context for each column we'll be copying
            final ChunkSource.FillContext oidfc     = ctx.makeFillContext(ordIdSource);
            final ChunkSource.FillContext poidfc    = ctx.makeFillContext(prevOrderIdSource);
            final ChunkSource.FillContext symfc     = ctx.makeFillContext(symSource);
            final ChunkSource.FillContext timefc    = ctx.makeFillContext(orderTimeSource);
            final ChunkSource.FillContext pricefc   = ctx.makeFillContext(priceSource);
            final ChunkSource.FillContext sizefc    = ctx.makeFillContext(sizeSource);
            final ChunkSource.FillContext esizefc   = ctx.makeFillContext(execSizeSource);
            final ChunkSource.FillContext sidefc    = ctx.makeFillContext(sideSource);
            final ChunkSource.FillContext opfc      = ctx.makeFillContext(opSource);


            // Now process the entire added index in chunks of CHUNK_SIZE (2048) rows.
            while(okit.hasMore()) {
                ctx.sc.reset();

                // Grab up to the next CHUNK_SIZE rows. nextKeys are row indices
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                // Copy the row data from the column sources into our processing chunks, using previous values if requested
                if(usePrev) {
                    ordIdSource.fillPrevChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    prevOrderIdSource.fillPrevChunk(poidfc, (WritableChunk<? super Values>) ctx.prevIdChunk, nextKeys);
                    symSource.fillPrevChunk(symfc, (WritableChunk<? super Values>) ctx.symChunk, nextKeys);
                    orderTimeSource.fillPrevChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                    priceSource.fillPrevChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    sizeSource.fillPrevChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    execSizeSource.fillPrevChunk(esizefc, (WritableChunk<? super Values>) ctx.execSizeChunk, nextKeys);
                    sideSource.fillPrevChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    opSource.fillPrevChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);

                } else {
                    ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    prevOrderIdSource.fillChunk(poidfc, (WritableChunk<? super Values>) ctx.prevIdChunk, nextKeys);
                    symSource.fillChunk(symfc, (WritableChunk<? super Values>) ctx.symChunk, nextKeys);
                    orderTimeSource.fillChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                    priceSource.fillChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    execSizeSource.fillChunk(esizefc, (WritableChunk<? super Values>) ctx.execSizeChunk, nextKeys);
                    sideSource.fillChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    opSource.fillChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);
                }


                final Instant timeNow = Instant.now();
                Instant now = Instant.now();

                long seconds = now.getEpochSecond();
                int nanos = now.getNano();

                long nowNanos = seconds * 1_000_000_000L + nanos;

                // Iterate over each row in the processing chunk,  and update the book.
                for(int ii = 0; ii< nextKeys.size(); ii++) {
                    // Get some minimal data
                    final long ordId = ctx.idChunk.get(ii);
                    final int op = ctx.opChunk.get(ii);
                    final int size = ctx.sizeChunk.get(ii);


                    /* Order book building logic
                     *
                     * Order Ack (OAK): Add order to book
                     * Order Cancel (CC): remove order from book
                     * Cancel-Replace (CRACK): Remove the order PREV_ORD_ID. Ack the new ORD_ID.
                     *                         If PREV_ORD_ID is 0, replace the size of ORD_ID
                     * Internal-Fill (INF): Subtract size from current order size
                     * Away-Fill (AWF): ??
                     *
                    */
                    switch (op) {
                        case OP_OAK -> {
                            final long existingOrderRow = orderMap.get(ordId);

                            // If the order doesn't already exist, add it
                            // TODO: Do we care if we try to ack an order already ack'd? Should we complain?
                            if (existingOrderRow == -1) {
                                // Get rest of order data
                                final String sym = ctx.symChunk.get(ii);
                                final long timestamp = ctx.timeChunk.get(ii);
                                final double price = ctx.priceChunk.get(ii);
                                final int side = ctx.sideChunk.get(ii);

                                this.addOrder(ctx, ordId, sym, timestamp, price, size, side, nowNanos);

                            } else {
                                System.out.println(ordId + " already exists, can't add order.");
                            }
                        }

                        case OP_CC -> this.removeOrder(ctx, ordId);

                        case OP_INF -> {
                            final long existingOrderRow = orderMap.get(ordId);
                            final int execSize = ctx.execSizeChunk.get(ii);
                            if (existingOrderRow != -1) {
                                this.modifyOrder(ctx, execSize, existingOrderRow, false, nowNanos);
                            } else {
                                System.out.println(ordId + " does not exist, can't modify order.");
                            }
                        }

                        // Remove the prevOrderId if there is one and add the orderId.
                        // If there is no prevOrderId, update the orderId with new size
                        case OP_CRAK -> {
                            final long prevOrderId = ctx.prevIdChunk.get(ii);
                            final long existingOrderRow = orderMap.get(ordId);

                            // We will replace the old order with a new one
                            if ( prevOrderId != 0) {
                                this.removeOrder(ctx, prevOrderId);

                                if (existingOrderRow == -1) {
                                    final String sym = ctx.symChunk.get(ii);
                                    final long timestamp = ctx.timeChunk.get(ii);
                                    final double price = ctx.priceChunk.get(ii);
                                    final int side = ctx.sideChunk.get(ii);

                                    this.addOrder(ctx, ordId, sym, timestamp, price, size, side, nowNanos);
                                } else {
                                    System.out.println(ordId + " already exists, can't add order.");
                                }

                            } else {
                                if (existingOrderRow != -1) {
                                    final int execSize = ctx.execSizeChunk.get(ii);
                                    this.modifyOrder(ctx, execSize, existingOrderRow, true, nowNanos);
                                } else {
                                    System.out.println(ordId + " does not exist, can't modify order.");
                                }

                            }
                        }
                    }

                }
            }
            resultUpdate.added = ctx.rowsAdded;
            resultUpdate.removed = ctx.rowsRemoved;
            resultUpdate.modified = ctx.rowsModified;
        }
    }

    /**
     * Process all rows from a book table snapshot
     *
     * @param t the snapshot of the book table
     * @return the Map of grouping keys to BookState
     */
    final void processInitBook(final Table t, TableUpdateImpl resultUpdate) {
        // Must be static
        assert !t.isRefreshing();

        final ColumnSource<Long> updateTimeSource = ReinterpretUtils.instantToLongSource(t.getColumnSource(UPDATE_TIME_NAME));
        final ColumnSource<Long> ordIdSource = t.getColumnSource(ORDID_NAME);
        final ColumnSource<String> symbolSource = t.getColumnSource(SYM_NAME);
        final ColumnSource<Long> ordTimeSource = ReinterpretUtils.instantToLongSource(t.getColumnSource(ORD_TIME_NAME));
        final ColumnSource<Double> priceSource = t.getColumnSource(PRC_NAME);
        final ColumnSource<Double> sizeSource = t.getColumnSource(SIZE_NAME);
        final ColumnSource<Double> sideSource = t.getColumnSource(SIDE_NAME);


        try(final InitContext context = new InitContext()) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = t.getRowSet().getRowSequenceIterator();

            final ChunkSource.FillContext uptimefc  = context.makeFillContext(updateTimeSource);
            final ChunkSource.FillContext oidfc     = context.makeFillContext(ordIdSource);
            final ChunkSource.FillContext symfc     = context.makeFillContext(symbolSource);
            final ChunkSource.FillContext ordtimefc = context.makeFillContext(ordTimeSource);
            final ChunkSource.FillContext pricefc   = context.makeFillContext(priceSource);
            final ChunkSource.FillContext sizefc    = context.makeFillContext(sizeSource);
            final ChunkSource.FillContext sidefc    = context.makeFillContext(sideSource);


            while (okit.hasMore()) {
                context.sc.reset();

                // Grab up to the next CHUNK_SIZE rows
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                updateTimeSource.fillChunk(uptimefc, (WritableChunk<? super Values>) context.updateTimeChunk, nextKeys);
                ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) context.idChunk, nextKeys);
                symbolSource.fillChunk(symfc, (WritableChunk<? super Values>) context.symChunk, nextKeys);
                ordTimeSource.fillChunk(ordtimefc, (WritableChunk<? super Values>) context.orderTimeChunk, nextKeys);
                priceSource.fillChunk(pricefc, (WritableChunk<? super Values>) context.priceChunk, nextKeys);
                sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) context.sizeChunk, nextKeys);
                sideSource.fillChunk(sidefc, (WritableChunk<? super Values>) context.sideChunk, nextKeys);

                for(int ii = 0; ii< nextKeys.size(); ii++) {
                    // Get some minimal data
                    final long updateTime = context.updateTimeChunk.get(ii);
                    final long ordId = context.idChunk.get(ii);
                    final String sym = context.symChunk.get(ii);
                    final long orderTime = context.orderTimeChunk.get(ii);
                    final int size = context.sizeChunk.get(ii);
                    final double price = context.priceChunk.get(ii);
                    final int side = context.sideChunk.get(ii);

                    // Add every order
                    // addOrder needs to accept both Contexts
                    this.addOrder(context, ordId, sym, orderTime, price, size, side, updateTime);
                }
            }

            // We are only adding rows when initing from snapshot
            resultUpdate.added = context.rowsAdded;
        }
    }


    /**
     * Add the specified order to the book.  If the order was new add it to the bookstate.
     *
     * @param ctx the context used to update the removed and added rowsets
     * @param orderId the order id
     * @param sym the symbol of the order
     * @param time the timestamp of the order
     * @param price the price of the order
     * @param size the size of the order
     * @param side the side of the order
     * @param timeNow the time the order is updated in the book
     *
     */
    private void addOrder(final BaseContext ctx,
                          final long orderId,
                          final String sym,
                          final long time,
                          final double price,
                          final int size,
                          final int side,
                          final long timeNow) {
        long rowOfAdded;
        boolean removeRemove = false;

        // Find an open slot or increment the row counter
        if (!availableRows.isEmpty()) {
            rowOfAdded = availableRows.iterator().nextLong();  // Grab any element
            availableRows.remove(rowOfAdded);  // Remove it from the set

            // If we are re-using a row index, we might have used it to remove in the same cycle
            removeRemove = true;

        } else {
            // No open spots, so expand the size of the columns
            rowOfAdded = resultSize;
            resultSize++;

            ordIdResults.ensureCapacity(resultSize);
            symResults.ensureCapacity(resultSize);
            orderTimeResults.ensureCapacity(resultSize);
            priceResults.ensureCapacity(resultSize);
            sizeResults.ensureCapacity(resultSize);
            sideResults.ensureCapacity(resultSize);
            updateTimeResults.ensureCapacity(resultSize);
        }

        //Add map from id to row num
        orderMap.put(orderId, rowOfAdded);
        ctx.addRow(rowOfAdded, removeRemove);

        ordIdResults.set(rowOfAdded, orderId);
        symResults.set(rowOfAdded, sym);
        orderTimeResults.set(rowOfAdded, time);
        priceResults.set(rowOfAdded, price);
        sizeResults.set(rowOfAdded, size);
        sideResults.set(rowOfAdded, side);
        updateTimeResults.set(rowOfAdded, timeNow);
    }

    /**
     * Remove the specified orderId from the book.
     *
     * @param ctx the context of the update cycle to update
     * @param orderId the order to remove
     *
     */
    private void removeOrder(Context ctx, long orderId) {
        final long rowOfRemoved;

        rowOfRemoved = orderMap.remove(orderId);

        if (rowOfRemoved != -1){
            availableRows.add(rowOfRemoved);
            ctx.removeRow(rowOfRemoved);
        } else {
            System.out.println(orderId + " does not exist, can't remove order.");
        }
    }

    /**
     * Modify the order at orderRow
     *
     * @param ctx the context of the update cycle to update
     * @param size the order to subtract from the current size
     * @param orderRow the row key to be modified
     * @param replace whether to replace or subtract the size from the original size
     *
     */
    private void modifyOrder(Context ctx, int size, long orderRow, boolean replace, long nowNanos) {

        if (replace) {
            sizeResults.set(orderRow, size);

        } else {
            int currSize = sizeResults.get(orderRow);
            sizeResults.set(orderRow, currSize - size);
        }

        updateTimeResults.set(orderRow, nowNanos);
        ctx.modifyRow(orderRow);
    }

    private interface BaseContext extends SafeCloseable {
        // Keep track of rows added, removed, or modified so that the result index can be updated and a downstream
        // update can be fired to anything listening to the result table.
        void addRow(long rowI, boolean removeRemove);
        ChunkSource.FillContext makeFillContext(ChunkSource<?> cs);
    }


    /**
     * This class holds various objects that are used during an update cycle.  This includes chunks for holding and processing
     * the updates, as well as the counts of rows added and a buffer for logging sorted prices.
     */
    private static class Context implements BaseContext {
        /*
         * Each of these WriteableChunks are used to process the update data more efficiently in linear chunks
         * instead of iterating over an index.  This avoids virtual method calls and is much more cache-friendly
         */
        final WritableLongChunk<?> idChunk;
        final WritableLongChunk<?> prevIdChunk;
        final WritableObjectChunk<String, ?> symChunk;
        final WritableLongChunk<?> timeChunk;
        final WritableDoubleChunk<?> priceChunk;
        final WritableIntChunk<?> sizeChunk;
        final WritableIntChunk<?> execSizeChunk;
        final WritableIntChunk<?> sideChunk;
        final WritableIntChunk<?> opChunk;

        WritableRowSet rowsAdded = RowSetFactory.empty();
        WritableRowSet rowsRemoved = RowSetFactory.empty();
        WritableRowSet rowsModified = RowSetFactory.empty();

        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(9);

        Context() {
            sc = SharedContext.makeSharedContext();

            idChunk         = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            prevIdChunk     = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            symChunk        = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
            timeChunk       = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            priceChunk      = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk       = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            execSizeChunk   = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            sideChunk       = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            opChunk         = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
        }


        public void addRow(long rowI, boolean removeRemove) {
            rowsAdded.insert(rowI);
            if (removeRemove) {
                rowsRemoved.remove(rowI);
            }
        }

        public void removeRow(long rowI) {
            rowsRemoved.insert(rowI);
            rowsAdded.remove(rowI);
            rowsModified.remove(rowI);
        }

        public void modifyRow(long rowI) {
            rowsModified.insert(rowI);
        }

        /**
         * At the end of an update cycle this must be invoked to close and release any shared resources that were claimed
         * during the update cycle.
         */
        @Override
        public void close() {
            sc.close();
            fillContexts.forEach(ChunkSource.FillContext::close);
            idChunk.close();
            prevIdChunk.close();
            symChunk.close();
            timeChunk.close();
            priceChunk.close();
            sizeChunk.close();
            execSizeChunk.close();
            sideChunk.close();
            opChunk.close();
        }

        /**
         * Just a helper method to create fill contexts and save them so they can be cleaned up neatly on close.
         *
         * @param cs the column source
         * @return a new fill context for that source.
         */
        public ChunkSource.FillContext makeFillContext(ChunkSource<?> cs) {
            final ChunkSource.FillContext fc = cs.makeFillContext(CHUNK_SIZE, sc);
            fillContexts.add(fc);
            return fc;
        }
    }

    /**
     * Specific context for processes a book snapshot
     */
    private static class InitContext implements BaseContext {
        /*
         * Each of these WriteableChunks are used to process the update data more efficiently in linear chunks
         * instead of iterating over an index.  This avoids virtual method calls and is much more cache-friendly
         */
        final WritableLongChunk<?> updateTimeChunk;
        final WritableLongChunk<?> idChunk;
        final WritableObjectChunk<String, ?> symChunk;
        final WritableLongChunk<?> orderTimeChunk;
        final WritableDoubleChunk<?> priceChunk;
        final WritableIntChunk<?> sizeChunk;
        final WritableIntChunk<?> sideChunk;

        WritableRowSet rowsAdded = RowSetFactory.empty();
        WritableRowSet rowsRemoved = RowSetFactory.empty();

        public void addRow(long rowI, boolean removeRemove) {
            rowsAdded.insert(rowI);
            if (removeRemove) {
                rowsRemoved.remove(rowI);
            }
        }


        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(7);

        InitContext() {
            sc = SharedContext.makeSharedContext();

            updateTimeChunk = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            idChunk         = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            symChunk        = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
            orderTimeChunk  = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            priceChunk      = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk       = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            sideChunk       = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
        }

        /**
         * At the end of an update cycle this must be invoked to close and release any shared resources that were claimed
         * during the update cycle.
         */
        @Override
        public void close() {
            sc.close();
            fillContexts.forEach(ChunkSource.FillContext::close);
            updateTimeChunk.close();
            idChunk.close();
            symChunk.close();
            orderTimeChunk.close();
            priceChunk.close();
            sizeChunk.close();
            sideChunk.close();
        }

        /**
         * Just a helper method to create fill contexts and save them so they can be cleaned up neatly on close.
         *
         * @param cs the column source
         * @return a new fill context for that source.
         */
        public ChunkSource.FillContext makeFillContext(ChunkSource<?> cs) {
            final ChunkSource.FillContext fc = cs.makeFillContext(CHUNK_SIZE, sc);
            fillContexts.add(fc);
            return fc;
        }
    }

    /**
     * This class listens for updates from the source table and pushes each row through the BookState for the symbol
     * indicated for the row, emitting a new row into the output table for each input row which results in a book
     * change.  This class processes the updates in chunks of 2048 rows for efficiency.
     */
    private class BookListener extends MergedListener implements Runnable  {
        final ListenerRecorder recorder;
        @SuppressWarnings("unchecked")
        public BookListener(@NotNull final ListenerRecorder recorder,
                            @NotNull final QueryTable source,
                            @NotNull final QueryTable result) {
            super(Collections.singleton(recorder), Collections.singletonList(source),
                    "BookListener", result);
            this.recorder = recorder;
        }

        @Override
        public void run() {
            // The Update Graph Processor will invoke this at the beginning of each cycle.  This lets the Book produce
            // an update to blink rows out, even if the upstream recorder did not produce any updates.
            // Without this,  we will violate Blink semantics by leaving the previous rows beyond the previous cycle.
            if (!result.isEmpty()) {
                notifyChanges();
            }
        }

        @Override
        protected void destroy() {
            // Be careful to clean up after ourselves to avoid leaking resources if this table gets closed and
            // dereferenced
            getUpdateGraph().removeSource(this);
            super.destroy();
        }

        @Override
        protected boolean canExecute(final long step) {
            // Only allow process() to be called if the source recorder has been satisfied. This could be if it has
            // itself fired, or it's upstream is satisfied and produced no updates.
            return getUpdateGraph().satisfied(step) && recorder.satisfied(step);
        }

        /**
         * Process an upstream update.  We only support added rows at this point, as handling removals and updates
         * would be very difficult to handle correctly with the book state.
         *
         */
        @Override
        public void process() {
            TableUpdateImpl resultUpdate = new TableUpdateImpl();

            // If the upstream listener has not fired (for example, if there were no upstream changes)
            // then do not try to process the updates -- there are none, we just need to blink the existing rows
            // out.
            if(recorder.recordedVariablesAreValid()) {
                final TableUpdate upstream = recorder.getUpdate();
                if (upstream.modified().isNonempty() ||
                        (upstream.removed().isNonempty() && !sourceIsBlink) ||
                        upstream.shifted().nonempty()) {
                    throw new IllegalStateException("BookBuilder is add only, but there were other updates");
                }

                // First process all of the new rows
                processAdded(upstream.added(), false, resultUpdate);

                // Handle the case where the input rows generate no book state changes,  we don't want to accidentally
                // try to inject a -1 into the row set.

            } else {
                resultUpdate.added = RowSetFactory.empty();
                resultUpdate.removed = RowSetFactory.empty();
                resultUpdate.modified = RowSetFactory.empty();
            }

            resultUpdate.shifted = RowSetShiftData.EMPTY;
            resultUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            // Once the rows have been processed then we create update the result index with the new rows and fire an
            // update for any downstream listeners of the result table.
            resultIndex.update(resultUpdate.added, resultUpdate.removed);
            resultTable.notifyListeners(resultUpdate);
        }
    }

    /**
     * Build a book of current live orders and their status
     *
     * @param source the table with the source data
     * @param idColumnName the name of the source order id column
     * @param symColumnName the name of the source order symbol column
     * @param timestampColumnName the name of the source timestamp column
     * @param priceColumnName the name of the source price column
     * @param sizeColumnName the name of the source size column
     * @param sideColumnName the name of the source side column
     * @param opColumnName the name of the source book-op column
     *
     * @return a new table representing the current state of the book.  This table will update as the source table updates.
     */
    @SuppressWarnings("unused")
    public static QueryTable build(@NotNull Table source,
                                   @NotNull String idColumnName,
                                   @NotNull String prevIdColumnName,
                                   @NotNull String symColumnName,
                                   @NotNull String timestampColumnName,
                                   @NotNull String priceColumnName,
                                   @NotNull String sizeColumnName,
                                   @NotNull String execSizeColumnName,
                                   @NotNull String sideColumnName,
                                   @NotNull String opColumnName,
                                   Table snapshot
                                   ) {
        final PriceBook book = new PriceBook(source,
                idColumnName,
                prevIdColumnName,
                symColumnName,
                timestampColumnName,
                priceColumnName,
                sizeColumnName,
                execSizeColumnName,
                sideColumnName,
                opColumnName,
                snapshot);

        return book.resultTable;
    }
}
