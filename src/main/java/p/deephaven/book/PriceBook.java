package p.deephaven.book;

import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.OperationSnapshotControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jetbrains.annotations.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Build a book of current live orders</p>
 * <p>
 *     Creating a price book is as simple as invoking the static build method, specifying the column names of
 *     your source table. There are 5 required columns and the rest will be passed through.
 *
 *      <pre>{@code
 *      import io.deephaven.book.PriceBook
 *
 *      orderStream = db.t("Market", "Orders")
 *                      .where("Date=`2021-01-13`", "Time > '2021-01-13T08:30:00 NY'")
 *                      .sort("Time")
 *
 *      book = PriceBook.build(orderStream,"ORD_ID", "PREV_ORD_ID", "QTY", "EXEC_QTY", "EVT_ID",
 *          "PassCol1", "PassCol2"...)
 *
 *      }</pre>
 *
 * <p></p>
 * <p>
 *
 *
 */
public class PriceBook {
    private static final int CHUNK_SIZE = 2048;

    private static final int OP_OAK = 1;
    private static final int OP_CC = 2;
    private static final int OP_INF = 3;
    private static final int OP_CRAK = 4;

    // Names of the output table columns
    private static final String UPDATE_TIME_NAME = "UpdateTimestamp";
    private static final String ORDID_NAME = "OrderId";
    private static final String SIZE_NAME = "Size";

    // region Input Sources
    private final ColumnSource<Long> ordIdSource;
    private final ColumnSource<Long> prevOrderIdSource;
    private final ColumnSource<Integer> sizeSource;
    private final ColumnSource<Integer> execSizeSource;
    private final ColumnSource<Integer> opSource;
    private final ColumnSource<?>[] passThroughSources;
    // endregion

    // region OutputSources
    final QueryTable resultTable;
    final BookListener bookListener;

    final TrackingWritableRowSet resultIndex;

    final InstantArraySource updateTimeResults;
    final IntegerArraySource sizeResults;
    final LongArraySource ordIdResults;
    final WritableColumnSource[] passThroughResults;
    final Class<?>[] passThroughClasses;
    final String[] passThroughColNames;
    // endregion

    // region Book state objects
    private final Long2LongOpenHashMap orderMap = new Long2LongOpenHashMap();
    private final LongArrayFIFOQueue  availableRows;
    private long  resultSize;
    // endregion

    private final boolean sourceIsBlink;

    private PriceBook(@NotNull final Table table,
                      final Table snapshot,
                      @NotNull String idColumnName,
                      @NotNull String prevIdColumnName,
                      @NotNull String sizeColumnName,
                      @NotNull String execSizeColumnName,
                      @NotNull String opColumnName,
                      String... passThroughCols) {

        final QueryTable source = (QueryTable) table.coalesce();
        this.sourceIsBlink = BlinkTableTools.isBlink(source);
        this.orderMap.defaultReturnValue(-1);
        this.passThroughSources = new ColumnSource[passThroughCols.length];
        this.passThroughResults = new WritableColumnSource[passThroughCols.length];
        this.passThroughClasses = new Class<?>[passThroughCols.length];
        this.passThroughColNames = passThroughCols;

        // Maybe make this configurable
        this.resultSize = 0;
        this.availableRows = new LongArrayFIFOQueue(16383);

        // Begin by getting references to the column sources from the input table to process later.
        this.ordIdSource        = source.getColumnSource(idColumnName);
        this.prevOrderIdSource  = source.getColumnSource(prevIdColumnName);
        this.sizeSource         = source.getColumnSource(sizeColumnName);
        this.execSizeSource     = source.getColumnSource(execSizeColumnName);
        this.opSource           = source.getColumnSource(opColumnName);

        // Construct the new column sources and result table.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Required output table columns
        updateTimeResults   = new InstantArraySource();
        ordIdResults        = new LongArraySource();
        sizeResults         = new IntegerArraySource();

        columnSourceMap.put(UPDATE_TIME_NAME, updateTimeResults);
        columnSourceMap.put(ORDID_NAME, ordIdResults);
        columnSourceMap.put(SIZE_NAME, sizeResults);

        // Add in the pass through column sources
        for (int ii = 0; ii < passThroughCols.length; ii++) {
            final String passColName = passThroughCols[ii];
            ColumnSource passColInputSource = source.getColumnSource(passColName);
            WritableColumnSource<?> passColResultSource;

            Class<?> colType = passColInputSource.getType();

            if (colType == Instant.class) {
                passColInputSource = ReinterpretUtils.instantToLongSource(passColInputSource);
                passColResultSource = new InstantArraySource();

            } else if (colType == long.class) {
                passColResultSource = new LongArraySource();

            } else if (colType == String.class) {
                passColResultSource = new ObjectArraySource(String.class);

            } else if (colType == double.class) {
                passColResultSource = new DoubleArraySource();

            } else if (colType == int.class) {
                passColResultSource = new IntegerArraySource();

            } else {
                throw new IllegalArgumentException(passColName + " is an invalid input column type: " + colType);
            }

            // Save the class for reference later
            // Add to pass through sources
            // Add to results for writing to later
            // Add to columnMap for constructing the results table.
            this.passThroughClasses[ii] = colType;
            this.passThroughSources[ii] = passColInputSource;
            this.passThroughResults[ii] = passColResultSource;
            columnSourceMap.put(passColName, this.passThroughResults[ii]);
        }

        // Finally, create the result table for the user
        final OperationSnapshotControl snapshotControl =
                source.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);

        final MutableObject<QueryTable> result = new MutableObject<>();
        final MutableObject<BookListener> listenerHolder = new MutableObject<>();
        final WritableRowSet snapAdded;

        if (snapshot != null) {

            if (!this.verifySnapshot(snapshot, columnSourceMap)) {
                throw new IllegalArgumentException("Snapshot schema does not match output schema");
            }
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

                    final TableUpdateImpl resultUpdate = new TableUpdateImpl();
                    processAdded(usePrev ? source.getRowSet().prev() : source.getRowSet(), usePrev, resultUpdate);

                    final WritableRowSet resultRows = (WritableRowSet) resultUpdate.added;
                    resultRows.insert(snapAdded);
                    resultRows.remove(resultUpdate.removed);

                    final QueryTable bookTable = new QueryTable((resultRows).toTracking(), columnSourceMap);

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
        if (source.isRefreshing()) {
            this.bookListener = listenerHolder.get();
            bookListener.getUpdateGraph().addSource(bookListener);
        } else {
            bookListener = null;
        }
    }

    private boolean verifySnapshot(Table snapshot, Map<String, ColumnSource<?>> resultSourceMap) {
        Map<String, ColumnSource<?>> snapshotSourceMap = (Map<String, ColumnSource<?>>) snapshot.getColumnSourceMap();

        if (resultSourceMap.keySet().equals(snapshotSourceMap.keySet())) {
            for (String colName: resultSourceMap.keySet()) {
                if (resultSourceMap.get(colName).getType() != snapshotSourceMap.get(colName).getType()) {
                    return false;
                }
            }
        } else {
            return false;
        }
        return true;
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
        try (final Context ctx = new Context(this.passThroughClasses)) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = added.getRowSequenceIterator();

            // In order to copy data into the writable chunks in the context we need to create
            // a fill context for each column we'll be copying
            final ChunkSource.FillContext oidfc     = ctx.makeFillContext(ordIdSource);
            final ChunkSource.FillContext poidfc    = ctx.makeFillContext(prevOrderIdSource);
            final ChunkSource.FillContext sizefc    = ctx.makeFillContext(sizeSource);
            final ChunkSource.FillContext esizefc   = ctx.makeFillContext(execSizeSource);
            final ChunkSource.FillContext opfc      = ctx.makeFillContext(opSource);

            // fc for pass through cols
            final ChunkSource.FillContext[] passfcs = Arrays.stream(this.passThroughSources)
                    .map(colSource -> ctx.makeFillContext(colSource)).toArray(ChunkSource.FillContext[]::new);

            final Instant timeNow = Instant.now();
            long seconds = timeNow.getEpochSecond();
            int nanos = timeNow.getNano();
            final long nowNanos = seconds * 1_000_000_000L + nanos;

            // Now process the entire added index in chunks of CHUNK_SIZE (2048) rows.
            while (okit.hasMore()) {
                ctx.sc.reset();

                // Grab up to the next CHUNK_SIZE rows. nextKeys are row indices
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                // Copy the row data from the column sources into our processing chunks, using previous values if requested
                if (usePrev) {
                    ordIdSource.fillPrevChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    prevOrderIdSource.fillPrevChunk(poidfc, (WritableChunk<? super Values>) ctx.prevIdChunk, nextKeys);
                    sizeSource.fillPrevChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    execSizeSource.fillPrevChunk(esizefc, (WritableChunk<? super Values>) ctx.execSizeChunk, nextKeys);
                    opSource.fillPrevChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);

                    for (int ii = 0; ii < passThroughSources.length; ii++) {
                        this.passThroughSources[ii].fillPrevChunk(passfcs[ii], (WritableChunk<? super Values>) ctx.passChunks[ii], nextKeys);
                    }
                } else {
                    ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    prevOrderIdSource.fillChunk(poidfc, (WritableChunk<? super Values>) ctx.prevIdChunk, nextKeys);
                    sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    execSizeSource.fillChunk(esizefc, (WritableChunk<? super Values>) ctx.execSizeChunk, nextKeys);
                    opSource.fillChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);

                    for (int ii = 0; ii < passThroughSources.length; ii++) {
                        this.passThroughSources[ii].fillChunk(passfcs[ii], (WritableChunk<? super Values>) ctx.passChunks[ii], nextKeys);
                    }
                }

                // Iterate over each row in the processing chunk, and update the book.
                for (int chunkI = 0; chunkI < nextKeys.size(); chunkI++) {
                    // Get some minimal data
                    final long ordId = ctx.idChunk.get(chunkI);
                    final int op = ctx.opChunk.get(chunkI);

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
                            if (existingOrderRow == -1) {
                                // Get rest of order data
                                final int size = ctx.sizeChunk.get(chunkI);

                                this.addOrder(ctx, ordId, size, nowNanos, chunkI);
                            }
                        }

                        case OP_CC -> this.removeOrder(ctx, ordId);

                        case OP_INF -> {
                            final long existingOrderRow = orderMap.get(ordId);
                            final int execSize = ctx.execSizeChunk.get(chunkI);

                            if (existingOrderRow != -1) {
                                // Should I pass this into modifyOrder?
                                int currSize = sizeResults.get(existingOrderRow);

                                // If size is down to 0, get rid of it
                                if (currSize - execSize == 0) {
                                    this.removeOrder(ctx, ordId);
                                } else {
                                    this.modifyOrder(ctx, execSize, existingOrderRow, false, timeNow);
                                }
                            }
                        }

                        // Remove the prevOrderId if there is one and add the orderId.
                        // If there is no prevOrderId, update the orderId with new size
                        case OP_CRAK -> {
                            final long prevOrderId = ctx.prevIdChunk.get(chunkI);
                            final int size = ctx.sizeChunk.get(chunkI);
                            final long existingOrderRow = orderMap.get(ordId);

                            // If ord_id exists, we only change the qty
                            if (existingOrderRow != -1 || prevOrderId == ordId) {
                                if (size == 0) {
                                    this.removeOrder(ctx, prevOrderId);
                                } else {
                                    this.modifyOrder(ctx, size, existingOrderRow, true, timeNow);
                                }

                            // If ord_id doesn't exist, we create a new order and remove the old order
                            // which has current prev_ord_id as its ord_id.
                            } else {
                                this.addOrder(ctx, ordId, size, nowNanos, chunkI);

                                if (prevOrderId != 0) {
                                    this.removeOrder(ctx, prevOrderId);
                                }
                            }
                        } default -> {
                            System.out.println("Ignoring invalid Op: " + op);
                        }
                    }
                }
            }
            resultUpdate.added = RowSetFactory.fromKeys(ctx.getAdded().toLongArray());
            resultUpdate.removed = RowSetFactory.fromKeys(ctx.getRemoved().toLongArray());
            resultUpdate.modified = RowSetFactory.fromKeys(ctx.getModified().toLongArray());
        }
    }

    /**
     * Add the specified order to the book.
     *
     * @param ctx the context used to update the removed and added rowsets
     * @param orderId the order id
     * @param size the size of the order
     * @param timeNow the time the order is updated in the book
     *
     */
    private void addOrder(BaseContext ctx,
                          long orderId,
                          int size,
                          long timeNow,
                          int chunkI) {
        long rowOfAdded;
        boolean inRemoved;

        // Find an open slot or increment the row counter
        if (!availableRows.isEmpty()) {
            rowOfAdded = availableRows.dequeueLong();  // Grab any element

            // Has this rowI been removed in this cycle?
            // If it was, remove from removed rowset
            // A remove followed by an add is a mod
            // Otherwise, just add to added rowset
            inRemoved = ctx.getRemoved().remove(rowOfAdded);
            if (inRemoved) {
                ctx.getModified().add(rowOfAdded);
            } else {
                ctx.getAdded().add(rowOfAdded);
            }

        } else {
            rowOfAdded = resultSize;
            resultSize++;

            ordIdResults.ensureCapacity(resultSize);
            sizeResults.ensureCapacity(resultSize);
            updateTimeResults.ensureCapacity(resultSize);

            for (int ii = 0; ii < passThroughResults.length; ii++) {
                this.passThroughResults[ii].ensureCapacity(resultSize);
            }

            ctx.getAdded().add(rowOfAdded);
        }

        //Add map from id to row num
        orderMap.put(orderId, rowOfAdded);

        ordIdResults.set(rowOfAdded, orderId);
        sizeResults.set(rowOfAdded, size);
        updateTimeResults.set(rowOfAdded, timeNow);

        for (int ii = 0; ii < passThroughResults.length; ii++) {
            Class<?> colType = this.passThroughClasses[ii];

            if (colType == Instant.class || colType == long.class) {
                this.passThroughResults[ii].set(rowOfAdded, ((WritableLongChunk<?>) ctx.getPassThroughChunks()[ii]).get(chunkI));

            } else if (colType == String.class) {
                this.passThroughResults[ii].set(rowOfAdded, ((WritableObjectChunk<String, ?>) ctx.getPassThroughChunks()[ii]).get(chunkI));

            } else if (colType == double.class) {
                this.passThroughResults[ii].set(rowOfAdded, ((WritableDoubleChunk<?>) ctx.getPassThroughChunks()[ii]).get(chunkI));

            } else if (colType == int.class) {
                this.passThroughResults[ii].set(rowOfAdded, ((WritableIntChunk<?>) ctx.getPassThroughChunks()[ii]).get(chunkI));

            } else {
                throw new IllegalArgumentException("Invalid input column type: " + colType);
            }
        }
    }

    /**
     * Remove the specified orderId from the book.
     *
     * @param ctx the context of the update cycle to update
     * @param orderId the order to remove
     *
     */
    private void removeOrder(Context ctx, long orderId) {
        final long rowOfRemoved = orderMap.remove(orderId);
        boolean inAdded;

        if (rowOfRemoved != -1) {
            // Only add to removed rowset if this was not added in this cycle
            // otherwise just remove from added
            inAdded = ctx.getAdded().remove(rowOfRemoved);
            if (!inAdded) {
                ctx.getRemoved().add(rowOfRemoved);
            }

            ctx.getModified().remove(rowOfRemoved);
            availableRows.enqueue(rowOfRemoved);
        }
    }

    /**
     * Modify the order at orderRow
     *
     * @param ctx the context of the update cycle to update
     * @param size the order to subtract from the current size
     * @param orderRow the row key to be modified
     * @param replace whether to replace or subtract the size from the original size
     * @param timeNow the time this will be updated
     *
     */
    private void modifyOrder(Context ctx, int size, long orderRow, boolean replace, Instant timeNow) {

        if (replace) {
            sizeResults.set(orderRow, size);

        } else {
            int currSize = sizeResults.get(orderRow);
            sizeResults.set(orderRow, currSize - size);
        }
        updateTimeResults.set(orderRow, timeNow);

        // Only add to mods if this rowI was not already added this cycle
        // If this row was added in this cycle, then this mod should be considered just an add (do nothing)
        if (!ctx.getAdded().contains(orderRow)) {
            ctx.getModified().add(orderRow);
        }
    }

    /**
     * Base context so we can use the order functions on either Init or regular Contexts
     */
    private interface BaseContext extends SafeCloseable {
        // Keep track of rows added, removed, or modified so that the result index can be updated and a downstream
        // update can be fired to anything listening to the result table.
        WritableChunk<?>[] getPassThroughChunks();

        LongOpenHashSet getAdded();

        LongOpenHashSet getRemoved();

        LongOpenHashSet getModified();

        ChunkSource.FillContext makeFillContext(ChunkSource<?> cs);
    }

    /**
     * This class holds various objects that are used during an update cycle.  This includes chunks for holding and processing
     * the updates, as well as the counts of rows added, removed and modified this update cycle.
     */
    private class Context implements BaseContext {
        /*
         * Each of these WriteableChunks are used to process the update data more efficiently in linear chunks
         * instead of iterating over an index.  This avoids virtual method calls and is much more cache-friendly
         */
        final WritableLongChunk<?> idChunk;
        final WritableLongChunk<?> prevIdChunk;
        final WritableIntChunk<?> sizeChunk;
        final WritableIntChunk<?> execSizeChunk;
        final WritableIntChunk<?> opChunk;
        // Map string to chunk or Array of chunks?

        WritableChunk<?>[] passChunks;

        LongOpenHashSet rowsAdded = new LongOpenHashSet();
        LongOpenHashSet rowsRemoved = new LongOpenHashSet();
        LongOpenHashSet rowsModified = new LongOpenHashSet();

        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(6);

        Context(Class<?>[] passThroughClasses) {
            sc = SharedContext.makeSharedContext();

            idChunk         = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            prevIdChunk     = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk       = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            execSizeChunk   = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            opChunk         = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);

            passChunks = new WritableChunk[passThroughClasses.length];

            // Add in the pass through column sources
            for (int ii = 0; ii < passThroughClasses.length; ii++) {
                Class<?> colType = passThroughClasses[ii];

                if (colType == Instant.class || colType == long.class) {
                    passChunks[ii] = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);

                } else if (colType == String.class) {
                    passChunks[ii] = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);

                } else if (colType == double.class) {
                    passChunks[ii] = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);

                } else if (colType == int.class) {
                    passChunks[ii] = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);

                } else {
                    throw new IllegalArgumentException("Invalid input column type: " + colType);
                }
            }
        }

        public LongOpenHashSet getAdded() {
            return this.rowsAdded;
        }

        public LongOpenHashSet getRemoved() {
            return this.rowsRemoved;
        }

        public LongOpenHashSet getModified() {
            return this.rowsModified;
        }

        public WritableChunk[] getPassThroughChunks() {
            return this.passChunks;
        }

        /**
         * At the end of an update cycle this must be invoked to close and release any shared resources that were claimed
         * during the update cycle.
         */
        @Override
        public void close() {
            sc.close();
            fillContexts.forEach(ChunkSource.FillContext::close);
            Arrays.stream(passChunks).forEach(SafeCloseable::close);
            idChunk.close();
            prevIdChunk.close();
            sizeChunk.close();
            execSizeChunk.close();
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
     * processInitBook and InitContext are specifically for processesing a snapshot
     * Process all rows from a book table snapshot
     *
     * @param t the snapshot of the book table
     * @param resultUpdate the update object that will be used in the listener to push out the added, removed,
     *                     and modified RowSets
     */
    final void processInitBook(final Table t, TableUpdateImpl resultUpdate) {
        // Must be static
        assert !t.isRefreshing();

        final ColumnSource<Long> updateTimeSource = ReinterpretUtils.instantToLongSource(t.getColumnSource(UPDATE_TIME_NAME));
        final ColumnSource<Long> ordIdSource = t.getColumnSource(ORDID_NAME);
        final ColumnSource<Double> sizeSource = t.getColumnSource(SIZE_NAME);

        // Get the sources from the snapshot.
        // Special care of Instants
        ColumnSource<?>[] snapPassSources = (ColumnSource<?>[]) Arrays.stream(this.passThroughColNames)
                .map(colName ->
                        (ColumnSource<?>) ((Class<?>) t.getColumnSource(colName).getType() == Instant.class
                                ? ReinterpretUtils.instantToLongSource(t.getColumnSource(colName)) :
                                t.getColumnSource(colName))
                ).toArray(ColumnSource<?>[]::new);

        try (final InitContext context = new InitContext(passThroughClasses)) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = t.getRowSet().getRowSequenceIterator();

            final ChunkSource.FillContext uptimefc  = context.makeFillContext(updateTimeSource);
            final ChunkSource.FillContext oidfc     = context.makeFillContext(ordIdSource);
            final ChunkSource.FillContext sizefc    = context.makeFillContext(sizeSource);

            final ChunkSource.FillContext[] passfcs = Arrays.stream(snapPassSources)
                    .map(colSource -> context.makeFillContext(colSource)).toArray(ChunkSource.FillContext[]::new);

            while (okit.hasMore()) {
                context.sc.reset();

                // Grab up to the next CHUNK_SIZE rows
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                updateTimeSource.fillChunk(uptimefc, (WritableChunk<? super Values>) context.updateTimeChunk, nextKeys);
                ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) context.idChunk, nextKeys);
                sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) context.sizeChunk, nextKeys);

                for (int ii = 0; ii < snapPassSources.length; ii++) {
                    snapPassSources[ii].fillChunk(passfcs[ii],
                            (WritableChunk<? super Values>) context.getPassThroughChunks()[ii], nextKeys);
                }

                for (int chunkI = 0; chunkI < nextKeys.size(); chunkI++) {
                    // Get some minimal data
                    final long updateTime = context.updateTimeChunk.get(chunkI);
                    final long ordId = context.idChunk.get(chunkI);
                    final int size = context.sizeChunk.get(chunkI);

                    // Add every order
                    // addOrder needs to accept both Contexts
                    this.addOrder(context, ordId, size, updateTime, chunkI);
                }
            }

            // We are only adding rows when initing from snapshot
            resultUpdate.added = RowSetFactory.fromKeys(context.rowsAdded.toLongArray());
        }
    }

    /**
     * Specific context for processes a book snapshot
     */
    private class InitContext implements BaseContext {
        /*
         * Each of these WriteableChunks are used to process the update data more efficiently in linear chunks
         * instead of iterating over an index.  This avoids virtual method calls and is much more cache-friendly
         */
        final WritableLongChunk<?> updateTimeChunk;
        final WritableLongChunk<?> idChunk;
        final WritableLongChunk<?> orderTimeChunk;
        final WritableIntChunk<?> sizeChunk;

        WritableChunk<?>[] passChunks;

        LongOpenHashSet rowsAdded = new LongOpenHashSet();
        LongOpenHashSet rowsRemoved = new LongOpenHashSet();
        LongOpenHashSet rowsModified = new LongOpenHashSet();

        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(7);

        InitContext(Class<?>[] passThroughClasses) {
            sc = SharedContext.makeSharedContext();

            updateTimeChunk = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            idChunk         = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            orderTimeChunk  = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk       = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);

            passChunks = new WritableChunk[passThroughClasses.length];

            // Add in the pass through column sources
            for (int ii = 0; ii < passThroughClasses.length; ii++) {
                Class<?> colType = passThroughClasses[ii];

                if (colType == Instant.class || colType == long.class) {
                    passChunks[ii] = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);

                } else if (colType == String.class) {
                    passChunks[ii] = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);

                } else if (colType == double.class) {
                    passChunks[ii] = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);

                } else if (colType == int.class) {
                    passChunks[ii] = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);

                } else {
                    throw new IllegalArgumentException("Invalid input column type: " + colType);
                }
            }
        }

        public LongOpenHashSet getAdded() {
            return this.rowsAdded;
        }

        public LongOpenHashSet getRemoved() {
            return this.rowsRemoved;
        }

        public LongOpenHashSet getModified() {
            return this.rowsModified;
        }

        public WritableChunk<?>[] getPassThroughChunks() {
            return this.passChunks;
        }

        /**
         * At the end of an update cycle this must be invoked to close and release any shared resources that were claimed
         * during the update cycle.
         */
        @Override
        public void close() {
            sc.close();
            fillContexts.forEach(ChunkSource.FillContext::close);
            Arrays.stream(passChunks).forEach(SafeCloseable::close);
            updateTimeChunk.close();
            idChunk.close();
            orderTimeChunk.close();
            sizeChunk.close();
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
            notifyChanges();
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

        @Override
        public void process() {
            TableUpdateImpl resultUpdate = new TableUpdateImpl();

            // If the upstream listener has not fired (for example, if there were no upstream changes)
            // then do not try to process the updates -- there are none, we just need to blink the existing rows
            // out.
            if (recorder.recordedVariablesAreValid()) {
                final TableUpdate upstream = recorder.getUpdate();
                if (upstream.modified().isNonempty()
                        || (upstream.removed().isNonempty() && !sourceIsBlink)
                        || upstream.shifted().nonempty()) {
                    throw new IllegalStateException("BookBuilder is add only, but there were other updates");
                }

                // First process all of the new rows
                processAdded(upstream.added(), false, resultUpdate);

                if (resultUpdate.modified.isEmpty()) {
                    resultUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                } else {
                    // Ideally make more specific
                    resultUpdate.modifiedColumnSet = ModifiedColumnSet.ALL;
                }

            } else {
                resultUpdate.added = RowSetFactory.empty();
                resultUpdate.removed = RowSetFactory.empty();
                resultUpdate.modified = RowSetFactory.empty();
                resultUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            }

            resultUpdate.shifted = RowSetShiftData.EMPTY;
            resultIndex.update(resultUpdate.added, resultUpdate.removed);
            // Once the rows have been processed then we create update the result index with the new rows and fire an
            // update for any downstream listeners of the result table.
            resultTable.notifyListeners(resultUpdate);
        }
    }

    // Helper function to print out an update object for debugging
    private String printUpdate(TableUpdateImpl update) {
        final RowSet removalsMinusPrevious = update.removed().minus(resultIndex.copyPrev());
        final RowSet addedMinusCurrent = update.added().minus(resultIndex);
        final RowSet removedIntersectCurrent = update.removed().intersect(resultIndex);
        final RowSet modifiedMinusCurrent = update.modified().minus(resultIndex);

        final RowSet addedIntersectPrevious = update.added().intersect(resultIndex.copyPrev());
        final RowSet modifiedMinusPrevious = update.modified().minus(resultIndex.copyPrev());

        // Everything is messed up for this table, print out the indices in an easy to understand way
        final LogOutput logOutput = new LogOutputStringImpl()
                .append("RowSet update: ")
                .append(LogOutput::nl).append("\t          previousIndex=").append(resultIndex.copyPrev())
                .append(LogOutput::nl).append("\t           currentIndex=").append(resultIndex)
                .append(LogOutput::nl).append("\t                  added=").append(update.added())
                .append(LogOutput::nl).append("\t                removed=").append(update.removed())
                .append(LogOutput::nl).append("\t               modified=").append(update.modified())
                .append(LogOutput::nl).append("\t           modifiedCols=").append(update.modifiedColumnSet().toString())
                .append(LogOutput::nl).append("\t                shifted=").append(update.shifted().toString())
                .append(LogOutput::nl).append("\t  removalsMinusPrevious=").append(removalsMinusPrevious)
                .append(LogOutput::nl).append("\t      addedMinusCurrent=").append(addedMinusCurrent)
                .append(LogOutput::nl).append("\t   modifiedMinusCurrent=").append(modifiedMinusCurrent)
                .append(LogOutput::nl).append("\tremovedIntersectCurrent=").append(removedIntersectCurrent)
                .append(LogOutput::nl).append("\t addedIntersectPrevious=").append(addedIntersectPrevious)
                .append(LogOutput::nl).append("\t  modifiedMinusPrevious=").append(modifiedMinusPrevious);

        return logOutput.toString();
    }

    /**
     * Build a book of current live orders and their status
     *
     * @param source the table with the source data
     * @param snapshot the optional snapshot to resume from
     * @param idColumnName the name of the source order id column
     * @param prevIdColumnName the name of the prev id column
     * @param sizeColumnName the name of the source size column
     * @param execSizeColumnName the name of the execution size column
     * @param opColumnName the name of the source book-op column
     * @param passThroughCols a list of column names to be pass through from to source to the book
     *
     * @return a new table representing the current state of the book.  This table will update as the source table updates.
     */
    @SuppressWarnings("unused")
    public static QueryTable build(@NotNull Table source,
                                   Table snapshot,
                                   @NotNull String idColumnName,
                                   @NotNull String prevIdColumnName,
                                   @NotNull String sizeColumnName,
                                   @NotNull String execSizeColumnName,
                                   @NotNull String opColumnName,
                                   @NotNull String... passThroughCols
                                   ) {
        final PriceBook book = new PriceBook(source,
                snapshot,
                idColumnName,
                prevIdColumnName,
                sizeColumnName,
                execSizeColumnName,
                opColumnName,
                passThroughCols);

        return book.resultTable;
    }
}
