package io.deephaven.book;

import java.lang.String;
import com.google.common.collect.MinMaxPriorityQueue;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.jetbrains.annotations.NotNull;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;


import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * <p>Build a book of the top N Bid/Ask prices in a columnar book format.</p>
 * <p>
 *     Creating a price book is as simple as invoking the static build method, specifying the columns to group on
 *     and the desired depth-of-book.
 *
 *      <pre>{@code
 *      import io.deephaven.book.PriceBook
 *
 *      orderStream = db.t("Market", "Orders")
 *                      .where("Date=`2021-01-13`", "Time > '2021-01-13T08:30:00 NY'")
 *                      .sort("Time")
 *
 *      // Construct a book of depth 8, grouping prices by their Symbol
 *      // This will use the default columns "Time", "ORIG_SIZE", "BUY_SELL_FLAG", "BOOK_OP", "PRICE"
 *      book = PriceBook.build(orderStream, 8, "Symbol")
 *      }</pre>
 *
 * <p>
 *     By default, the PriceBook will group input rows that have identical timestamps into a single emitted output row.
 *     If this is not the desired behavior, or you require more fine grained control of the columns used to build the book
 *     you may use the more specific builder method
 *     {@link #build(Table, boolean, String, String, String, String, String, String, String)}
 * </p>
 * <p></p>
 * <p>
 *
 */
public class PriceBook {
    private static final int CHUNK_SIZE = 2048;
    private static final int SIDE_BUY = 0;
    private static final int SIDE_SELL = 1;

    private static final int OP_INSERT = 1;
    private static final int OP_REMOVE = 2;

    private static final String PRC_NAME = "Price";
    private static final String TIME_NAME = "Timestamp";
    private static final String SIZE_NAME = "Size";
    private static final String SYM_NAME = "Symbol";
    private static final String ORDID_NAME = "OrderId";
    private static final String SIDE_NAME = "Side";

    private final BookState state = new BookState();
    private final boolean batchTimestamps;

    // region Input Sources
    private final ColumnSource<Long> timeSource;
    private final ColumnSource<Double> priceSource;
    private final ColumnSource<Integer> sizeSource;
    private final ColumnSource<Integer> sideSource;
    private final ColumnSource<Integer> opSource;
    private final ColumnSource<String> symSource;

    private final ColumnSource<Long> ordIdSource;
    // endregion

    // region OutputSources
    final QueryTable resultTable;
    final BookListener bookListener;

    final TrackingWritableRowSet resultIndex;

    final InstantArraySource timeResult;
    final DoubleArraySource priceResults;
    final InstantArraySource timeResults;
    final IntegerArraySource sizeResults;
    final IntegerArraySource sideResults;
    final LongArraySource ordIdResults;
    final ObjectArraySource<String> symResults;

    // endregion

    private final boolean sourceIsBlink;

    private PriceBook(@NotNull final Table table,
                      final boolean batchTimestamps,
                      @NotNull String timestampColumnName,
                      @NotNull String sizeColumnName,
                      @NotNull String sideColumnName,
                      @NotNull String opColumnName,
                      @NotNull String priceColumnName,
                      @NotNull String symColumnName,
                      @NotNull String idColumnName) {
        final QueryTable source = (QueryTable) table.coalesce();
        this.batchTimestamps = batchTimestamps;
        this.sourceIsBlink = BlinkTableTools.isBlink(source);

        // Begin by getting references to the column sources from the input table to process later.
        this.timeSource = ReinterpretUtils.instantToLongSource(source.getColumnSource(timestampColumnName));
        this.priceSource = source.getColumnSource(priceColumnName);
        this.sizeSource = source.getColumnSource(sizeColumnName);
        this.sideSource = source.getColumnSource(sideColumnName);
        this.opSource = source.getColumnSource(opColumnName);
        this.symSource = source.getColumnSource(symColumnName);
        // Order ids will be the keys
        this.ordIdSource = source.getColumnSource(idColumnName);

        // Construct the new column sources and result table.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Now we create the columns which will be in the output table.
        // TODO: Get rid of timeResult?
        timeResult = new InstantArraySource();
        columnSourceMap.put("Timestamp", timeResult);

        // Is this right? Should I get rid of the whole concept of a keySource?
        columnSourceMap.put(ORDID_NAME, this.ordIdSource);

        // Dont do this because the key is just always going to be order id
//        keyOutputSources = new WritableColumnSource[groupingCols.length];
//        for(int ii = 0; ii < groupingCols.length; ii++) {
//            final String groupingColName = groupingCols[ii];
//            final ColumnSource<?> gsCol = source.getColumnSource(groupingColName);
//            keyOutputSources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(gsCol.getType(), gsCol.getComponentType());
//            columnSourceMap.put(groupingColName, keyOutputSources[ii]);
//        }

        // Set output table columns
        priceResults = new DoubleArraySource();
        timeResults = new InstantArraySource();
        sizeResults = new IntegerArraySource();
        sideResults = new IntegerArraySource();
        // We will want the sym source, right? What kind of source?
        symResults = new ObjectArraySource(String.class);
        ordIdResults = new LongArraySource();

        columnSourceMap.put(PRC_NAME, priceResults);
        columnSourceMap.put(TIME_NAME, timeResults);
        columnSourceMap.put(SIZE_NAME, sizeResults);
        columnSourceMap.put(SIDE_NAME, sideResults);
        columnSourceMap.put(SYM_NAME, symResults);
        columnSourceMap.put(ORDID_NAME, ordIdResults);

        // Finally, create the result table for the user
        final OperationSnapshotControl snapshotControl =
                source.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);

        final MutableObject<QueryTable> result = new MutableObject<>();
        final MutableObject<BookListener> listenerHolder = new MutableObject<>();
        QueryTable.initializeWithSnapshot("bookBuilder", snapshotControl,
                (prevRequested, beforeClock) -> {
                    final boolean usePrev = prevRequested && source.isRefreshing();

                    // Initialize the internal state by processing the entire input table.  This will be done asynchronously from
                    // the LTM thread and so it must know if it should use previous values or current values.
                    // TODO: Need to make sure rowsAdded still makes sense
                    final long rowsAdded = processAdded(usePrev ? source.getRowSet().prev() : source.getRowSet(), usePrev);

                    final QueryTable bookTable = new QueryTable(
                            (rowsAdded == 0 ? RowSetFactory.empty() : RowSetFactory.fromRange(0, rowsAdded - 1)).toTracking()
                            , columnSourceMap);
                    if (snapshotControl != null) {
                        columnSourceMap.values().forEach(ColumnSource::startTrackingPrevValues);
                        bookTable.setRefreshing(true);
                        bookTable.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
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
     * @return the number of new rows emitted to the result.
     */
    @SuppressWarnings("unchecked")
    private long processAdded(RowSet added, boolean usePrev) {
        // First create the context object in a try-with-resources so it gets automatically cleaned up when we're done.
        try(final Context ctx = new Context()) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = added.getRowSequenceIterator();

            // In order to copy data into the writable chunks in the context we need to create
            // a fill context for each column we'll be copying/
            final ChunkSource.FillContext timefc = ctx.makeFillContext(timeSource);
            final ChunkSource.FillContext pricefc = ctx.makeFillContext(priceSource);
            final ChunkSource.FillContext sizefc = ctx.makeFillContext(sizeSource);
            final ChunkSource.FillContext sidefc = ctx.makeFillContext(sideSource);
            final ChunkSource.FillContext opfc = ctx.makeFillContext(opSource);
            final ChunkSource.FillContext symfc = ctx.makeFillContext(symSource);
            // Keys are order ids
            final ChunkSource.FillContext oidfc = ctx.makeFillContext(ordIdSource);

            // Only one state
            BookState currentState = this.state;
            long lastTime = -1;
            boolean batchUpdated = false;

            // Now process the entire added index in chunks of CHUNK_SIZE (2048) rows.
            while(okit.hasMore()) {
                ctx.sc.reset();

                // Grab up to the next CHUNK_SIZE rows. nextKeys are row indices
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                // Copy the row data from the column sources into our processing chunks, using previous values if requested
                if(usePrev) {
                    ordIdSource.fillPrevChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    opSource.fillPrevChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);
                    sideSource.fillPrevChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    sizeSource.fillPrevChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    priceSource.fillPrevChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    ordIdSource.fillPrevChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    timeSource.fillPrevChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                    symSource.fillPrevChunk(symfc, (WritableChunk<? super Values>) ctx.symChunk, nextKeys);
                } else {
                    ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    opSource.fillChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);
                    sideSource.fillChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    priceSource.fillChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    timeSource.fillChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                    symSource.fillChunk(symfc, (WritableChunk<? super Values>) ctx.symChunk, nextKeys);
                }

                // Iterate over each row in the processing chunk,  and update the book.
                for(int ii = 0; ii< nextKeys.size(); ii++) {

                    // Now grab each of the relevant values from the row and push them through the book to update.
                    final double price = ctx.priceChunk.get(ii);
                    final int size = ctx.sizeChunk.get(ii);
                    final int side = ctx.sideChunk.get(ii);
                    final int op = ctx.opChunk.get(ii);
                    final long timestamp = ctx.timeChunk.get(ii);
                    final long ordId = ctx.idChunk.get(ii);
                    final String sym = ctx.symChunk.get(ii);

                    // Update the internal state
                    // TODO: When should batchUpdated be true vs false? how to handle remove vs add?
                    batchUpdated |= currentState.update(timestamp, price, size, side, ordId, op);

                    // We should only log a row if we are either not batching,  or the timestamp changed.
                    // Is this still true?
                    final boolean logRowGate = (!batchTimestamps || timestamp != lastTime);

                    // Update the output table
                    if(batchUpdated &&
                            logRowGate &&
                            currentState.isOrderInBook(ordId)) {

                        recordChange(ctx, currentState, timestamp, ordId, price, size, side, sym);
                    }

                    // If we logged a row reset the updated flag.
                    if(logRowGate) {
                        batchUpdated = false;
                    }
                    lastTime = timestamp;
                }
            }

            return ctx.rowsAdded;
        }
    }

    /**
     * Write out a single row of updates to the output table.
     *
     * @param ctx the context object
     * @param state the book that was updated
     * @param timestamp the timestamp of the change
     * @param ordId the ordId being updated
     * @param price the price being updated
     * @param size the size being updated
     * @param side the side being updated
     * @param sym the sym being updated
     */
    public void recordChange(Context ctx, BookState state, long timestamp, long ordId, double price, int size, int side, String sym) {

        // Get the index from the state. Does this mess up rowsAdded?
        final long nextIndex = state.orderMap.get(ordId);
        // What is the new capacity now? Is this fine?
        final long newSize = nextIndex + 1;

        // Write the appropriate output values to each column source, first ensuring that each column is big enough
        // to hold the next value.
        // TODO: Whats up with timeResult vs timeResults?
//        timeResult.ensureCapacity(newSize);
//        timeResult.set(nextIndex, timestamp);

        // What do I do here?
        timeResults.ensureCapacity(newSize);
        timeResults.set(nextIndex, timestamp);

        priceResults.ensureCapacity(newSize);
        priceResults.set(nextIndex, price);

        sizeResults.ensureCapacity(newSize);
        sizeResults.set(nextIndex, size);

        ordIdResults.ensureCapacity(newSize);
        ordIdResults.set(nextIndex, ordId);

        sideResults.ensureCapacity(newSize);
        sideResults.set(nextIndex, side);

        symResults.ensureCapacity(newSize);
        symResults.set(nextIndex, sym);



//        for(int ii = 0; ii < keyOutputSources.length; ii++) {
//            keyOutputSources[ii].ensureCapacity(newSize);
//            keyOutputSources[ii].set(nextIndex, keySource.exportElement(key, ii));
//        }
//
//        // Fill out the bid columns
//        fillFrom(0, depth, nextIndex, newSize, state.bids, ctx, Comparator.reverseOrder());
//
//        // Fill out the ask columns
//        fillFrom(depth, depth, nextIndex, newSize, state.asks, ctx, Comparator.naturalOrder());


        // Increment the number of rows added.
        // TODO: Is this still relevant?
        ctx.rowsAdded++;
    }

    /**
     * Fill out a set of timestamp/price/size columns in order.  These columns were created in order in the constructor
     * for all bids, and then asks,  so we can just walk 'depth' times to write out the prices.
     *
     * @param start the starting index of best prices
     * @param count the number of prices to write
     * @param destination the destination row to write to in the output table
     * @param newSize the new table size for ensureCapcity
     * @param book the book to source data from
     * @param ctx the shared context
     * @param comparator the comparator for sorting the prices properly.
     */
    //TODO:  No longer needed???
//    private void fillFrom(int start, int count, long destination, long newSize, Book book, Context ctx, Comparator<? super Double> comparator) {
//        // First copy the best prices from the book into our temporary array and sort it appropriately
//
//        // Then, once for each price level  write out the timestamp, size, and price of that particular order from the book.
//        for(int ii = start; ii < start + count; ii++) {
//            final double price;
//            final int size;
//            final long time;
//
//            // Select the values to be written.  Initially, the book may not be fully populated so we will
//            // use NULL values for each type for the remaining values.
//            if(ii - start < book.bestPrices.size()) {
//                price = ctx.priceBuf[ii-start];
//                time = book.timestampMap.get(price);
//                size = book.sizeMap.get(price);
//            } else {
//                price = NULL_DOUBLE;
//                time = NULL_LONG;
//                size = NULL_INT;
//            }
//
//            priceResults[ii].ensureCapacity(newSize);
//            priceResults[ii].set(destination, price);
//
//            timeResults[ii].ensureCapacity(newSize);
//            timeResults[ii].set(destination, time);
//
//            sizeResults[ii].ensureCapacity(newSize);
//            sizeResults[ii].set(destination, size);
//        }
//    }

    /**
     * A Book simply tracks a set of prices and sizes.  It uses two min/max priority queues.  One maintains the top
     * N items,  the other maintains the backlog of prices.  Prices are added to the bestPrices queue until it is full,
     * then as prices come in, if they belong in bestPrices then the last item is moved from the bestPrices to the backlog queue
     * and the new item is inserted into the bestPrices queue.
     *
     * This class uses a comparator so that the same code can be used for Bids which need to be sorted descending, and asks
     * which need to be sorted ascending.
     */
    private static class BookState {
        private final Long2LongOpenHashMap orderMap = new Long2LongOpenHashMap();
        private final LongOpenHashSet  availableRows = new LongOpenHashSet();
        private long  nextRow;


        BookState() {
            this.orderMap.defaultReturnValue(-1);
            this.nextRow = 0;
        }

        /**
         * Update the book state with the specified price.  If the book op was DELETE or REMOVE, or the size was 0
         * the price will be removed from the book.
         *
         * @param time the time of the price
         * @param price the price
         * @param size the size of the order
         * @param side the side of the order
         * @param orderId the id of the order
         * @param op the book op
         * @return true if the price resulted in a book update
         */
        public boolean update(final long time,
                              final double price,
                              final int size,
                              final int side,
                              final long orderId,
                              final int op) {

            // Remove this price from the book entirely.
            if(op == OP_REMOVE || size == 0) {
                return this.removeFrom(orderId);
            }

            return this.addOrder(orderId);
        }

        /**
         * Update the specified order in the book.  If the order was new add it to the bookstate.
         *
         * @param orderId the order id
         * @return true of the price was added
         *
         * If you find a match in the map, you are done
         * If not, see if you have a reclaimed row you can use,  use that
         * If not, get and increment the counter and use that row.
         * Insert the row from (a,b,c) into the output tables RowSet
         * Write / Read values in the ColumnSources at whichever row number you got from (a,b,c)
         *
         */
        private boolean addOrder(long orderId) {
            long rowI;

            // We tried to add an order we already have.
            if (orderMap.containsKey(orderId)) {
                return false;
            }

            if (!availableRows.isEmpty()) {
                rowI = availableRows.iterator().nextLong();  // Grab any element
                availableRows.remove(rowI);  // Remove it from the set

            } else {
                rowI = nextRow;
                nextRow++;
            }

            //Add map from id to row num
            orderMap.put(orderId, rowI);

            // TODO: Add to output table RowSet?
            // (Done in recordChange I think)

            return true;
        }

        /**
         * Remove the specified orderId from the book.
         *
         * @param orderId the order to remove
         * @return true if anything was actually removed.
         */
        private boolean removeFrom(long orderId) {
            final long rowOfRemoved;
            final long rowNum;

            rowOfRemoved = orderMap.remove(orderId);

            if (rowOfRemoved == -1){
                return false;
            } else {
                //TODO: remove it from the table’s RowSet
                // (Done in recordChange I think? how?)
                availableRows.add(rowOfRemoved);
                return true;
            }
        }

        /**
         * Check if the specified orderId is in the book.
         *
         * @param orderId the price
         * @return true if it is in the top N prices.
         */
        public boolean isOrderInBook(long orderId) {
            return orderMap.containsKey(orderId);
        }
    }



    /**
     * This class holds various objects that are used during an update cycle.  This includes chunks for holding and processing
     * the updates, as well as the counts of rows added and a buffer for logging sorted prices.
     */
    private static class Context implements SafeCloseable {
        /*
         * Each of these WriteableChunks are used to process the update data more efficiently in linear chunks
         * instead of iterating over an index.  This avoids virtual method calls and is much more cache-friendly
         */
        final WritableLongChunk<?> timeChunk;
        final WritableDoubleChunk<?> priceChunk;
        final WritableIntChunk<?> sizeChunk;
        final WritableIntChunk<?> sideChunk;
        final WritableIntChunk<?> opChunk;
        final WritableLongChunk<?> idChunk;
        final WritableObjectChunk<String, ?> symChunk;

        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(6);


        // rowsAdded keeps track of how many update rows were emitted so that the result index can be updated and a downstream
        // update can be fired to anything listening to the result table.
        long rowsAdded = 0;

        Context() {
            sc = SharedContext.makeSharedContext();

            timeChunk  = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            priceChunk = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk  = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            sideChunk  = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            opChunk    = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            idChunk   = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            symChunk   = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
        }

        /**
         * At the end of an update cycle this must be invoked to close and release any shared resources that were claimed
         * during the update cycle.
         */
        @Override
        public void close() {
            sc.close();
            fillContexts.forEach(ChunkSource.FillContext::close);
            timeChunk.close();
            priceChunk.close();
            sizeChunk.close();
            sideChunk.close();
            opChunk.close();
            idChunk.close();
            symChunk.close();
        }

        /**
         * Just a helper method to create fill contexts and save them so they can be cleaned up neatly on close.
         *
         * @param cs the column source
         * @return a new fill context for that source.
         */
        ChunkSource.FillContext makeFillContext(ChunkSource<?> cs) {
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
            final RowSet addedIndex;
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
                final long rowsAdded = processAdded(upstream.added(), false);

                // Handle the case where the input rows generate no book state changes,  we don't want to accidentally
                // try to inject a -1 into the row set.
                addedIndex = rowsAdded == 0 ? RowSetFactory.empty() : RowSetFactory.fromRange(0, rowsAdded - 1);
            } else {
                addedIndex = RowSetFactory.empty();
            }

            // Once the rows have been processed then we create update the result index with the new rows and fire an
            // update for any downstream listeners of the result table.
            final RowSet removedIndex = resultIndex.copy();
            resultIndex.update(addedIndex, removedIndex);
            resultTable.notifyListeners(new TableUpdateImpl(addedIndex,
                    removedIndex,
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY));
        }
    }

    /**
     * Build a book of bid and ask prices with the specified number of levels from the requested table, grouping input rows by the
     * specified set of grouping columns.  Levels will be represented as a set of columns (Price, Time, Size) for each level.
     *
     * @param source the table with the source data
     * @param batchTimestamps set to true to batch input rows with identical timestamps into the a single output row.
     * @param timestampColumnName the name of the source timestamp column
     * @param sizeColumnName the name of the source size column
     * @param sideColumnName the name of the source side column
     * @param opColumnName the name of the source book-op column
     * @param priceColumnName the name of the price column
     * @param
     *
     * @return a new table representing the current state of the book.  This table will update as the source table updates.
     */
    @SuppressWarnings("unused")
    public static QueryTable build(@NotNull Table source,
                                   boolean batchTimestamps,
                                   @NotNull String timestampColumnName,
                                   @NotNull String sizeColumnName,
                                   @NotNull String sideColumnName,
                                   @NotNull String opColumnName,
                                   @NotNull String priceColumnName,
                                   @NotNull String symColumnName,
                                   @NotNull String idColumnName) {
        final PriceBook book = new PriceBook(source,
                batchTimestamps,
                timestampColumnName,
                sizeColumnName,
                sideColumnName,
                opColumnName,
                priceColumnName,
                symColumnName,
                idColumnName);

        return book.resultTable;
    }
}
