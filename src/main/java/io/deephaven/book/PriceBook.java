package io.deephaven.book;

import java.lang.String;
import java.time.Instant;
import com.google.common.collect.MinMaxPriorityQueue;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.impl.AdaptiveRowSetBuilderRandom;
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
 *     {@link #build(Table, String, String, String, String, String, String, String)}
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

    private static final String ORDID_NAME = "OrderId";
    private static final String SYM_NAME = "Symbol";
    private static final String TIME_NAME = "Timestamp";
    private static final String PRC_NAME = "Price";
    private static final String SIZE_NAME = "Size";
    private static final String SIDE_NAME = "Side";


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

    final InstantArraySource updateTimeResult;
    final DoubleArraySource priceResults;
    final InstantArraySource timeResults;
    final IntegerArraySource sizeResults;
    final IntegerArraySource sideResults;
    final LongArraySource ordIdResults;
    final ObjectArraySource<String> symResults;

    // endregion

    // region from BookState

    private final Long2LongOpenHashMap orderMap = new Long2LongOpenHashMap();
    private final LongOpenHashSet  availableRows = new LongOpenHashSet();
    private long  nextRow;

    // endregion

    private final boolean sourceIsBlink;

    private PriceBook(@NotNull final Table table,
                      @NotNull String idColumnName,
                      @NotNull String symColumnName,
                      @NotNull String timestampColumnName,
                      @NotNull String priceColumnName,
                      @NotNull String sizeColumnName,
                      @NotNull String sideColumnName,
                      @NotNull String opColumnName) {
        final QueryTable source = (QueryTable) table.coalesce();
        this.sourceIsBlink = BlinkTableTools.isBlink(source);
        this.orderMap.defaultReturnValue(-1);
        this.nextRow = 0;

        // Begin by getting references to the column sources from the input table to process later.
        // Order ids will be the keys
        this.ordIdSource = source.getColumnSource(idColumnName);
        this.symSource = source.getColumnSource(symColumnName);
        this.timeSource = ReinterpretUtils.instantToLongSource(source.getColumnSource(timestampColumnName));
        this.priceSource = source.getColumnSource(priceColumnName);
        this.sizeSource = source.getColumnSource(sizeColumnName);
        this.sideSource = source.getColumnSource(sideColumnName);
        this.opSource = source.getColumnSource(opColumnName);

        // Construct the new column sources and result table.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Now we create the columns which will be in the output table.
        // Column for the update time
        updateTimeResult = new InstantArraySource();
        columnSourceMap.put("UpdateTimestamp", updateTimeResult);

        // Set output table columns
        ordIdResults = new LongArraySource();
        symResults = new ObjectArraySource(String.class);
        timeResults = new InstantArraySource();
        priceResults = new DoubleArraySource();
        sizeResults = new IntegerArraySource();
        sideResults = new IntegerArraySource();

        columnSourceMap.put(ORDID_NAME, ordIdResults);
        columnSourceMap.put(SYM_NAME, symResults);
        columnSourceMap.put(TIME_NAME, timeResults);
        columnSourceMap.put(PRC_NAME, priceResults);
        columnSourceMap.put(SIZE_NAME, sizeResults);
        columnSourceMap.put(SIDE_NAME, sideResults);

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

                    // TODO: so processAdded now returns a ctx, which will contain the 3 rowsets. Is that ok? Will that
                    //  mess with the 'try'-with-resources statement?
                    final Context initCtx = processAdded(usePrev ? source.getRowSet().prev() : source.getRowSet(), usePrev);


                    final QueryTable bookTable = new QueryTable( (initCtx.rowsAdded.build()).toTracking(), columnSourceMap);

                    if (snapshotControl != null) {
                        columnSourceMap.values().forEach(ColumnSource::startTrackingPrevValues);
                        bookTable.setRefreshing(true);

                        // TODO: The output resultsTable, should NOT be a blink table, right? because now I'm removing
                        //  and modifying things...?
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
     * @return the Context used for processing
     */
    @SuppressWarnings("unchecked")
    private Context processAdded(RowSet added, boolean usePrev) {
        // First create the context object in a try-with-resources so it gets automatically cleaned up when we're done.
        try(final Context ctx = new Context()) {
            // Next we get an iterator into the added index so that we can process the update in chunks.
            final RowSequence.Iterator okit = added.getRowSequenceIterator();

            // In order to copy data into the writable chunks in the context we need to create
            // a fill context for each column we'll be copying
            final ChunkSource.FillContext oidfc = ctx.makeFillContext(ordIdSource);
            final ChunkSource.FillContext symfc = ctx.makeFillContext(symSource);
            final ChunkSource.FillContext timefc = ctx.makeFillContext(timeSource);
            final ChunkSource.FillContext pricefc = ctx.makeFillContext(priceSource);
            final ChunkSource.FillContext sizefc = ctx.makeFillContext(sizeSource);
            final ChunkSource.FillContext sidefc = ctx.makeFillContext(sideSource);
            final ChunkSource.FillContext opfc = ctx.makeFillContext(opSource);


            // Now process the entire added index in chunks of CHUNK_SIZE (2048) rows.
            while(okit.hasMore()) {
                ctx.sc.reset();

                // Grab up to the next CHUNK_SIZE rows. nextKeys are row indices
                final RowSequence nextKeys = okit.getNextRowSequenceWithLength(CHUNK_SIZE);

                // Copy the row data from the column sources into our processing chunks, using previous values if requested
                if(usePrev) {
                    ordIdSource.fillPrevChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    symSource.fillPrevChunk(symfc, (WritableChunk<? super Values>) ctx.symChunk, nextKeys);
                    timeSource.fillPrevChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                    priceSource.fillPrevChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    sizeSource.fillPrevChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    sideSource.fillPrevChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    opSource.fillPrevChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);

                } else {
                    ordIdSource.fillChunk(oidfc, (WritableChunk<? super Values>) ctx.idChunk, nextKeys);
                    symSource.fillChunk(symfc, (WritableChunk<? super Values>) ctx.symChunk, nextKeys);
                    timeSource.fillChunk(timefc, (WritableChunk<? super Values>) ctx.timeChunk, nextKeys);
                    priceSource.fillChunk(pricefc, (WritableChunk<? super Values>) ctx.priceChunk, nextKeys);
                    sizeSource.fillChunk(sizefc, (WritableChunk<? super Values>) ctx.sizeChunk, nextKeys);
                    sideSource.fillChunk(sidefc, (WritableChunk<? super Values>) ctx.sideChunk, nextKeys);
                    opSource.fillChunk(opfc, (WritableChunk<? super Values>) ctx.opChunk, nextKeys);

                }


                // TODO: Should I do the now() now, or right when I set the result in addOrder?
                //  OR at the top of processAdded?
                Instant timeNow = Instant.now();

                // Iterate over each row in the processing chunk,  and update the book.
                for(int ii = 0; ii< nextKeys.size(); ii++) {

                    // Now grab each of the relevant values from the row and push them through the book to update.
                    final long ordId = ctx.idChunk.get(ii);
                    final String sym = ctx.symChunk.get(ii);
                    final long timestamp = ctx.timeChunk.get(ii);
                    final double price = ctx.priceChunk.get(ii);
                    final int size = ctx.sizeChunk.get(ii);
                    final int side = ctx.sideChunk.get(ii);
                    final int op = ctx.opChunk.get(ii);

                    // Update the internal state AND the result table
                    this.update(ctx, ordId, sym, timestamp, price, size, side, op, timeNow);
                }
            }

            // Just return the whole ctx and get added/removed/modified from there
            return ctx;
        }
    }


    /**
     * Update the book state with the specified order + operation.
     * If the book op was DELETE or REMOVE, or the size was 0, the order will be removed from the book.
     *
     *
     * @param time the time of the price
     * @param price the price
     * @param size the size of the order
     * @param side the side of the order
     * @param orderId the id of the order
     * @param op the book op
     * @return true if the price resulted in a book update
     */
    public void update(final Context ctx,
                          final long orderId,
                          final String sym,
                          final long time,
                          final double price,
                          final int size,
                          final int side,
                          final int op,
                          final Instant timeNow) {

        // Will add fill, update, etc op logic here

        // Remove this price from the book entirely.
        // TODO: I'm not sure about size=0, might be subject to personal preference?
        if(op == OP_REMOVE || size == 0) {
            this.removeFrom(ctx, orderId);
        }

        // TODO: Is it bad that I'm just passing all these input parameters through multiple functions?
        this.addOrder(ctx, orderId, sym, time, price, size, side, timeNow);
    }

    /**
     * Update the specified order in the book.  If the order was new add it to the bookstate.
     *
     * @param orderId the order id
     *
     * If you find a match in the map, you are done
     * If not, see if you have a reclaimed row you can use,  use that
     * If not, get and increment the counter and use that row.
     * Insert the row from (a,b,c) into the output tables RowSet
     * Write / Read values in the ColumnSources at whichever row number you got from (a,b,c)
     *
     */
    private void addOrder(Context ctx,
                          long orderId,
                          String sym,
                          long time,
                          double price,
                          int size,
                          int side,
                          Instant timeNow) {
        long rowOfAdded;
        long newSize;

        // We tried to add an order we already have.
        if (orderMap.containsKey(orderId)) {
            return;
        }

        // Find an open slot or increment the row counter
        if (!availableRows.isEmpty()) {
            rowOfAdded = availableRows.iterator().nextLong();  // Grab any element
            availableRows.remove(rowOfAdded);  // Remove it from the set

        } else {
            rowOfAdded = nextRow;
            nextRow++;
        }

        //Add map from id to row num
        orderMap.put(orderId, rowOfAdded);
        ctx.rowsAdded.addKey(rowOfAdded);

        // Add to results
        newSize = rowOfAdded + 1;

        ordIdResults.ensureCapacity(newSize);
        ordIdResults.set(rowOfAdded, orderId);

        symResults.ensureCapacity(newSize);
        symResults.set(rowOfAdded, sym);

        timeResults.ensureCapacity(newSize);
        timeResults.set(rowOfAdded, time);

        priceResults.ensureCapacity(newSize);
        priceResults.set(rowOfAdded, price);

        sizeResults.ensureCapacity(newSize);
        sizeResults.set(rowOfAdded, size);

        sideResults.ensureCapacity(newSize);
        sideResults.set(rowOfAdded, side);

        updateTimeResult.ensureCapacity(newSize);
        updateTimeResult.set(rowOfAdded, timeNow);
    }

    /**
     * Remove the specified orderId from the book.
     *
     * @param orderId the order to remove
     *
     */
    private void removeFrom(Context ctx, long orderId) {
        final long rowOfRemoved;

        rowOfRemoved = orderMap.remove(orderId);

        if (rowOfRemoved != -1){
            availableRows.add(rowOfRemoved);
            ctx.rowsRemoved.addKey(rowOfRemoved);
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

    /**
     * This class holds various objects that are used during an update cycle.  This includes chunks for holding and processing
     * the updates, as well as the counts of rows added and a buffer for logging sorted prices.
     */
    private static class Context implements SafeCloseable {
        /*
         * Each of these WriteableChunks are used to process the update data more efficiently in linear chunks
         * instead of iterating over an index.  This avoids virtual method calls and is much more cache-friendly
         */
        final WritableLongChunk<?> idChunk;
        final WritableObjectChunk<String, ?> symChunk;
        final WritableLongChunk<?> timeChunk;
        final WritableDoubleChunk<?> priceChunk;
        final WritableIntChunk<?> sizeChunk;
        final WritableIntChunk<?> sideChunk;
        final WritableIntChunk<?> opChunk;

        /*
         * The SharedContext and FillContexts are used by the column sources when they copy data into the chunks
         * above in order to share resources within a single update cycle.
         */
        final SharedContext sc;
        final List<ChunkSource.FillContext> fillContexts = new ArrayList<>(6);


        // rowsAdded keeps track of how many update rows were emitted so that the result index can be updated and a downstream
        // update can be fired to anything listening to the result table.
        RowSetBuilderRandom rowsAdded = RowSetFactory.builderRandom();
        RowSetBuilderRandom rowsRemoved = RowSetFactory.builderRandom();
        RowSetBuilderRandom rowsModified = RowSetFactory.builderRandom();

        Context() {
            sc = SharedContext.makeSharedContext();

            idChunk   = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            symChunk   = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
            timeChunk  = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            priceChunk = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
            sizeChunk  = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            sideChunk  = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            opChunk    = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
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
            symChunk.close();
            timeChunk.close();
            priceChunk.close();
            sizeChunk.close();
            sideChunk.close();
            opChunk.close();
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
            final RowSet removedIndex;
            final RowSet modifiedIndex;
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
                final Context ctx = processAdded(upstream.added(), false);

                // Handle the case where the input rows generate no book state changes,  we don't want to accidentally
                // try to inject a -1 into the row set.
                addedIndex = ctx.rowsAdded.build();
                removedIndex = ctx.rowsRemoved.build();
                modifiedIndex = ctx.rowsModified.build();

            } else {
                addedIndex = RowSetFactory.empty();
                removedIndex = RowSetFactory.empty();
                modifiedIndex = RowSetFactory.empty();
            }

            // Once the rows have been processed then we create update the result index with the new rows and fire an
            // update for any downstream listeners of the result table.
            resultIndex.update(addedIndex, removedIndex);
            resultTable.notifyListeners(new TableUpdateImpl(addedIndex,
                    removedIndex,
                    modifiedIndex,
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY));
        }
    }

    /**
     * Build a book of bid and ask prices with the specified number of levels from the requested table, grouping input rows by the
     * specified set of grouping columns.  Levels will be represented as a set of columns (Price, Time, Size) for each level.
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
                                   @NotNull String symColumnName,
                                   @NotNull String timestampColumnName,
                                   @NotNull String priceColumnName,
                                   @NotNull String sizeColumnName,
                                   @NotNull String sideColumnName,
                                   @NotNull String opColumnName
                                   ) {
        final PriceBook book = new PriceBook(source,
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
