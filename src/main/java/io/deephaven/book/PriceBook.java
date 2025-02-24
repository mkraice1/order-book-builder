package io.deephaven.book;


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
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import org.jetbrains.annotations.NotNull;

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
 *     {@link #build(Table, int, boolean, String, String, String, String, String, String...)}
 * </p>
 * <p></p>
 * <p>
 *      The following example creates a book of depth 5, that does NOT group identical timestamps, and groups input rows by "Symbol" and "Exchange"
 *      <pre>{@code
 *      book = PriceBook.build(orderStream, 5, false, "Timestamp", "OrderSize", "OrderSize", "BookOp", "Price", "Symbol", "Exchange")
 *      }</pre>
 *
 */
public class PriceBook {
    private static final int CHUNK_SIZE = 2048;
    private static final int SIDE_BUY = 0;
    private static final int SIDE_SELL = 1;

    private static final int OP_INSERT = 1;
    private static final int OP_REMOVE = 2;

    private final Map<Object, BookState> states = new HashMap<>();
    private final boolean batchTimestamps;
    private final int depth;

    // region Input Sources
    private final ColumnSource<Long> timeSource;
    private final ColumnSource<Double> priceSource;
    private final ColumnSource<Integer> sizeSource;
    private final ColumnSource<Integer> sideSource;
    private final ColumnSource<Integer> opSource;

    private final TupleSource keySource;
    // endregion

    // region OutputSources
    final QueryTable resultTable;
    final BookListener bookListener;

    final TrackingWritableRowSet resultIndex;

    final WritableColumnSource[] keyOutputSources;
    final InstantArraySource timeResult;
    final DoubleArraySource[] priceResults;
    final InstantArraySource[] timeResults;
    final IntegerArraySource[] sizeResults;

    // endregion

    private final boolean sourceIsBlink;

    private PriceBook(@NotNull final Table table,
                      final int depth,
                      final boolean batchTimestamps,
                      @NotNull String timestampColumnName,
                      @NotNull String sizeColumnName,
                      @NotNull String sideColumnName,
                      @NotNull String opColumnName,
                      @NotNull String priceColumnName,
                      @NotNull String... groupingCols) {
        final QueryTable source = (QueryTable) table.coalesce();
        this.batchTimestamps = batchTimestamps;
        this.depth = depth;
        this.sourceIsBlink = BlinkTableTools.isBlink(source);

        // Begin by getting references to the column sources from the input table to process later.
        this.timeSource = source.getColumnSource(timestampColumnName).reinterpret(long.class);
        this.priceSource = source.getColumnSource(priceColumnName);
        this.sizeSource = source.getColumnSource(sizeColumnName);
        this.sideSource = source.getColumnSource(sideColumnName);
        this.opSource = source.getColumnSource(opColumnName);

        // Since we may group by more than one column (say Symbol, Exchange) we want to create a single key object to look into
        // the book state map.  Packing the key sources into a tuple does this neatly.
        this.keySource = TupleSourceFactory.makeTupleSource(Arrays.stream(groupingCols).map(source::getColumnSource).toArray(ColumnSource[]::new));

        // Construct the new column sources and result table.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Now we create the columns which will be in the output table
        timeResult = new InstantArraySource();
        columnSourceMap.put("Timestamp", timeResult);

        keyOutputSources = new WritableColumnSource[groupingCols.length];
        for(int ii = 0; ii < groupingCols.length; ii++) {
            final String groupingColName = groupingCols[ii];
            final ColumnSource<?> gsCol = source.getColumnSource(groupingColName);
            keyOutputSources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(gsCol.getType(), gsCol.getComponentType());
            columnSourceMap.put(groupingColName, keyOutputSources[ii]);
        }

        // The number of Price/Timestamp/Size columns is twice the requested book depth,  one set for
        // bids, and one set for asks.
        priceResults = new DoubleArraySource[2 * depth];
        timeResults = new InstantArraySource[2 * depth];
        sizeResults = new IntegerArraySource[2 * depth];

        // Create each one iteratively, generate the name, and stuff them into the result column source map.
        for(int ii = 0; ii < 2 * depth; ii++) {
            final String prefix = ii < depth ? "Bid" : "Ask";
            final String suffix = Integer.toString((ii % depth) + 1);
            final InstantArraySource ts = new InstantArraySource();
            timeResults[ii] = ts;
            columnSourceMap.put(prefix+"_Timestamp"+ suffix, ts);

            final DoubleArraySource ps = new DoubleArraySource();
            priceResults[ii] = ps;
            columnSourceMap.put(prefix+"_Price"+suffix, ps);

            final IntegerArraySource ss = new IntegerArraySource();
            sizeResults[ii] = ss;
            columnSourceMap.put(prefix+"_Size"+suffix, ss);
        }

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
        try(final Context ctx = new Context(depth)) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = added.getRowSequenceIterator();

            // In order to copy data into the writable chunks in the context we need to create
            // a fill context for each column we'll be copying/
            final ChunkSource.FillContext timefc = ctx.makeFillContext(timeSource);
            final ChunkSource.FillContext pricefc = ctx.makeFillContext(priceSource);
            final ChunkSource.FillContext sizefc = ctx.makeFillContext(sizeSource);
            final ChunkSource.FillContext sidefc = ctx.makeFillContext(sideSource);
            final ChunkSource.FillContext opfc = ctx.makeFillContext(opSource);
            final ChunkSource.FillContext keyfc = ctx.makeFillContext(keySource);

            BookState currentState = null;
            Object currentKey = null;
            long lastTime = -1;
            boolean batchUpdated = false;

            // Now process the entire added index in chunks of CHUNK_SIZE (2048) rows.
            while(okit.hasMore()) {
                ctx.sc.reset();

                // Grab up to the next CHUNK_SIZE rows
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                // Copy the row data from the column sources into our processing chunks, using previous values if requested
                if(usePrev) {
                    keySource.fillPrevChunk(keyfc, (WritableChunk<? super Values>) ctx.keyChunk, nextKeys);
                    opSource.fillPrevChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);
                    sideSource.fillPrevChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    sizeSource.fillPrevChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    priceSource.fillPrevChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    timeSource.fillPrevChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                } else {
                    keySource.fillChunk(keyfc, (WritableChunk<? super Values>) ctx.keyChunk, nextKeys);
                    opSource.fillChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);
                    sideSource.fillChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    priceSource.fillChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    timeSource.fillChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                }

                // Iterate over each row in the processing chunk,  and update the appropriate book.
                for(int ii = 0; ii< nextKeys.size(); ii++) {
                    // First get the key that identifies which book this price belongs to
                    final Object nextKey = ctx.keyChunk.get(ii);

                    // Then fetch the book if it's different than the last key.  This lets us avoid a bunch of
                    // hash lookups
                    if(currentState == null || !Objects.equals(currentKey, nextKey)) {
                        // Maybe record the last batch if we were batching.
                        if(batchTimestamps && batchUpdated) {
                            // If the book indicated there was an update, and the given price was one of the top N
                            // bid or ask prices, we will emit a row to the output.
                            recordChange(ctx, currentState, lastTime, currentKey);
                            batchUpdated = false;
                        }

                        // Try to avoid hash lookups..
                        currentState = states.computeIfAbsent(nextKey, k -> new BookState(depth));
                        currentKey = nextKey;
                    }

                    // Now grab each of the relevant values from the row and push them through the book to update.
                    final double price = ctx.priceChunk.get(ii);
                    final int size = ctx.sizeChunk.get(ii);
                    final int side = ctx.sideChunk.get(ii);
                    final int op = ctx.opChunk.get(ii);
                    final long timestamp = ctx.timeChunk.get(ii);

                    batchUpdated |= currentState.update(timestamp, price, size, side, op);

                    // We should only log a row if we are either not batching,  or the timestamp changed
                    final boolean logRowGate = (!batchTimestamps || timestamp != lastTime);

                    if(batchUpdated &&
                            logRowGate &&
                            ((side == SIDE_BUY  && currentState.bids.isPriceInBook(price)) ||
                                    (side == SIDE_SELL && currentState.asks.isPriceInBook(price))   )) {
                        // If the book indicated there was an update, and the given price was one of the top N
                        // bid or ask prices, we will emit a row to the output.
                        recordChange(ctx, currentState, timestamp, currentKey);
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
     * @param key the key being updated
     */
    public void recordChange(Context ctx, BookState state, long timestamp, Object key) {
        // The next index to write to is the current size of the index plus however many rows we have added so far.
        final long nextIndex = ctx.rowsAdded;
        final long newSize = nextIndex + 1;

        // Write the appropriate output values to each column source, first ensuring that each column is big enough
        // to hold the next value.
        timeResult.ensureCapacity(newSize);
        timeResult.set(nextIndex, timestamp);

        for(int ii = 0; ii < keyOutputSources.length; ii++) {
            keyOutputSources[ii].ensureCapacity(newSize);
            keyOutputSources[ii].set(nextIndex, keySource.exportElement(key, ii));
        }

        // Fill out the bid columns
        fillFrom(0, depth, nextIndex, newSize, state.bids, ctx, Comparator.reverseOrder());

        // Fill out the ask columns
        fillFrom(depth, depth, nextIndex, newSize, state.asks, ctx, Comparator.naturalOrder());

        // Increment the number of rows added.
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
    private void fillFrom(int start, int count, long destination, long newSize, Book book, Context ctx, Comparator<? super Double> comparator) {
        // First copy the best prices from the book into our temporary array and sort it appropriately
        Arrays.sort(book.bestPrices.toArray(ctx.priceBuf), 0, book.bestPrices.size(), comparator);

        // Then, once for each price level  write out the timestamp, size, and price of that particular order from the book.
        for(int ii = start; ii < start + count; ii++) {
            final double price;
            final int size;
            final long time;

            // Select the values to be written.  Initially, the book may not be fully populated so we will
            // use NULL values for each type for the remaining values.
            if(ii - start < book.bestPrices.size()) {
                price = ctx.priceBuf[ii-start];
                time = book.timestampMap.get(price);
                size = book.sizeMap.get(price);
            } else {
                price = NULL_DOUBLE;
                time = NULL_LONG;
                size = NULL_INT;
            }

            priceResults[ii].ensureCapacity(newSize);
            priceResults[ii].set(destination, price);

            timeResults[ii].ensureCapacity(newSize);
            timeResults[ii].set(destination, time);

            sizeResults[ii].ensureCapacity(newSize);
            sizeResults[ii].set(destination, size);
        }
    }

    /**
     * A Book simply tracks a set of prices and sizes.  It uses two min/max priority queues.  One maintains the top
     * N items,  the other maintains the backlog of prices.  Prices are added to the bestPrices queue until it is full,
     * then as prices come in, if they belong in bestPrices then the last item is moved from the bestPrices to the backlog queue
     * and the new item is inserted into the bestPrices queue.
     *
     * This class uses a comparator so that the same code can be used for Bids which need to be sorted descending, and asks
     * which need to be sorted ascending.
     */
    private static class Book {
        private final MinMaxPriorityQueue<Double> bestPrices;
        private final PriorityQueue<Double> overflowPrices;
        private final Double2IntOpenHashMap sizeMap = new Double2IntOpenHashMap();
        private final Double2LongOpenHashMap timestampMap = new Double2LongOpenHashMap();

        private final long depth;
        private final Comparator<Double> comparator;

        Book(int depth, Comparator<Double> comparator) {
            this.depth = depth;
            this.comparator = comparator;
            this.sizeMap.defaultReturnValue(-1);
            this.timestampMap.defaultReturnValue(-1);
            bestPrices = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(depth).create();
            overflowPrices = new PriorityQueue<>(comparator);
        }

        /**
         * Update the specified price in the book.  If the price was new and larger than the any of the prices in the
         * bestPrices queue, the last price in the bestPrices queue is moved to the backlog and the new price is inserted
         * into the bestPrices, otherwise it is inserted directly into the backlog.
         *
         * @param price the price to update
         * @param size the order size
         * @param time the order time
         * @return true of the price was added
         */
        private boolean updatePrice(double price, int size, long time) {
            final long prevSize = sizeMap.put(price, size);
            final long prevTime = timestampMap.put(price, time);

            // It's a new price!
            if(prevSize == sizeMap.defaultReturnValue()) {
                if(bestPrices.size() < depth) {
                    bestPrices.offer(price);
                } else if(comparator.compare(price, bestPrices.peekLast()) < 0) {
                    // Move the lowest value from the top10 to the overflow
                    overflowPrices.offer(bestPrices.pollLast());

                    // Push this price onto the top10
                    bestPrices.offer(price);
                } else {
                    overflowPrices.offer(price);
                }

                return true;
            }

            return prevSize != size || prevTime != time;
        }

        /**
         * Remove the specified price from the book.  If the price was one of the top N prices then the next
         * price will be promoted from the backlog queue.
         *
         * @param price the price to remove
         * @return true if anything was actually removed.
         */
        private boolean removeFrom(double price) {
            final boolean wasRemoved;

            if(comparator.compare(price, bestPrices.peekLast()) <= 0) {
                // Remove the price
                wasRemoved = bestPrices.remove(price);

                // Promote one from the overflow
                if(wasRemoved && !overflowPrices.isEmpty()) {
                    bestPrices.offer(overflowPrices.poll());
                }
            } else {
                wasRemoved = overflowPrices.remove(price);
            }

            sizeMap.remove(price);
            timestampMap.remove(price);

            return wasRemoved;
        }

        /**
         * Check if the specified price is in the top N prices defined by the book depth.
         *
         * @param price the price
         * @return true if it is in the top N prices.
         */
        public boolean isPriceInBook(double price) {
            return bestPrices.size() < depth || comparator.compare(price, bestPrices.peekLast()) <= 0;
        }
    }

    /**
     * A convenient state holder object that holds a bid and ask book.  Upon updates, this will decide if the update was
     * an UPDATE, ADD, REMOVE, or DELETE, as well as which side of the order it was and update the proper book.
     */
    private static class BookState {
        final Book bids;
        final Book asks;

        BookState(int depth) {
            bids = new Book(depth, Comparator.reverseOrder());
            asks = new Book(depth, Comparator.naturalOrder());
        }

        /**
         * Update the book state with the specified price.  If the book op was DELETE or REMOVE, or the size was 0
         * the price will be removed from the book.
         *
         * @param time the time of the price
         * @param price the price
         * @param size the size of the order
         * @param side the side of the order
         * @param op the book op
         * @return true if the price resulted in a book update
         */
        public boolean update(final long time,
                              final double price,
                              final int size,
                              final int side,
                              final int op) {
            final Book book = (side == SIDE_SELL) ? asks: bids;

            // Remove this price from the book entirely.
            if(op == OP_REMOVE || size == 0) {
                return book.removeFrom(price);
            }

            return book.updatePrice(price, size, time);
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
        final WritableObjectChunk<?, ? extends Values> keyChunk;

        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(6);

        /*
         * The price buf is used to output the sorted bestN orders.  Since we are using PriorityQueue, which is a min heap
         * there is way to walk the best N prices directly.  In order to avoid creating a bunch of array garbage for every
         * row we emit,  this is used.
         */
        final Double[] priceBuf;

        // rowsAdded keeps track of how many update rows were emitted so that the result index can be updated and a downstream
        // update can be fired to anything listening to the result table.
        long rowsAdded = 0;

        Context(int depth) {
            priceBuf  = new Double[depth];
            sc = SharedContext.makeSharedContext();

            timeChunk  = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            priceChunk = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk  = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            sideChunk  = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            opChunk    = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            keyChunk   = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
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
            keyChunk.close();
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
     * @param depth the desired book depth
     * @param batchTimestamps set to true to batch input rows with identical timestamps into the a single output row.
     * @param timestampColumnName the name of the source timestamp column
     * @param sizeColumnName the name of the source size column
     * @param sideColumnName the name of the source side column
     * @param opColumnName the name of the source book-op column
     * @param priceColumnName the name of the price column
     * @param groupingCols the columns to group the source table by
     *
     * @return a new table representing the current state of the book.  This table will update as the source table updates.
     */
    @SuppressWarnings("unused")
    public static QueryTable build(@NotNull Table source,
                                   int depth,
                                   boolean batchTimestamps,
                                   @NotNull String timestampColumnName,
                                   @NotNull String sizeColumnName,
                                   @NotNull String sideColumnName,
                                   @NotNull String opColumnName,
                                   @NotNull String priceColumnName,
                                   @NotNull String... groupingCols) {
        final PriceBook book = new PriceBook(source,
                depth,
                batchTimestamps,
                timestampColumnName,
                sizeColumnName,
                sideColumnName,
                opColumnName,
                priceColumnName,
                groupingCols);

        return book.resultTable;
    }
}
