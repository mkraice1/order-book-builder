import p.deephaven.book.PriceBook

import io.deephaven.csv.CsvTools
import io.deephaven.engine.table.impl.replay.Replayer
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.enterprise.database.SystemTableLogger
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


prepareData = {startTime, eventsStream ->
    // Align timestamps to start_time
    nowTime = startTime
    nowNanos = startTime.getEpochSecond()*1000000000 + startTime.getNano()

    seconds = nowNanos / 1_000_000_000;
    nanosOfSecond = nowNanos % 1_000_000_000;

    eventsStream = eventsStream.sort("EPOCH_TS").updateView("FakeTime = Instant.ofEpochSecond( (long) (nowNanos / 1_000_000_000), (nowNanos % 1_000_000_000)).plusNanos(ii*2000)")

    return eventsStream
}


//   ====== PREPARE DATA ======
"""
Load and transform the csv data. Still static.
I am using 'FakeTime' so I have full control over the speed of ingestion.
It will currently spit out .5M rows/second
"""
if (!binding.hasVariable('events')) {
    events = CsvTools.readCsv("/tmp/data/event.csv")
}

// Align data with a made up timestamp "FakeTime"
tradeDate = today()
startTime = now()
endTime = startTime.plusSeconds(60*5)
eventsStatic = prepareData(startTime, events)
        .updateView("Date = `${tradeDate}`", "MyChar = (char) 'A'")


// ====== STREAM + PROCESS DATA WHILE SNAPSHOTTING ======
"""
Here we start streaming the prepared data, run it through the book builder, and save snapshots along the way
Snapshotting could be done in a separate worker if it is interfering with performance too much.
It performs fine, so I have not done that.
"""
rp = new Replayer(startTime, endTime)
eventsTicking = rp.replay(eventsStatic, "FakeTime")
rp.start()

passCols = ["SYMB", "SIDE", "PRC", "EPOCH_TS"]
defaultCols = ["ORD_ID", "PREV_ORD_ID", "QTY", "EXEC_QTY", "EVT_ID"]
currBook = PriceBook.build(eventsTicking, null, *defaultCols, *passCols)

// Save snaps with a java scheduler.
// (10 seconds just to get some quick snaps. In practice you might want it more spaced out)
snapFreq = 10

// This time column is used to choose the exact moment to resume the source data from!
// In practice, this should probably be "EPOCH_TS", but I'm using "FakeTime" for convinience
TIME_COL = "FakeTime"

defaultCtx = ExecutionContext.getContext()

opts = SystemTableLogger.newOptionsBuilder()
        .currentDateColumnPartition(true)
        .build()

scheduler = Executors.newScheduledThreadPool(1)
task = {
    println "Saving Snap"
    try (SafeCloseable ignored = defaultCtx.open()) {
        defaultCtx.getUpdateGraph().exclusiveLock().doLocked(() -> {
            snapTime = now().toString()
            lastTickTime = eventsTicking.getColumnSource(TIME_COL).get(eventsTicking.getRowSet().lastRowKey()).toString()
            toAppend = currBook.update("SnapTime = '${snapTime}'", "LastTickTime = '${lastTickTime}'")
            SystemTableLogger.logTable(db, "BookStates", "FlatBook", toAppend, opts)
        });
    }
} as Runnable

scheduler.scheduleAtFixedRate(task, 0, snapFreq, TimeUnit.SECONDS)


// ====== DISABLE SCHEDULER (optional) ======
""" 
After ~30 seconds or so, you may want to remove the handle. Or you can let it run until the end of the data.
You might also want to view the snapshots that were taken.
"""
scheduler.shutdown()
allSnaps = db.liveTable("BookStates", "FlatBook").where("Date = `${tradeDate}`")
snapTimes = allSnaps.selectDistinct("SnapTime")


// ====== PREPARE RESUME ======
"""
Specify a time and date you want to resume from.
Get the book snapshot right before that time and filter the source 
    to after the last tick time that the book had processed.
"""
prepareResume = {String tradeDate, String tradeTime, Table source ->
    // Get first snapshot right before the given time
    def resumeTime = ZonedDateTime.parse(tradeDate + "T" + tradeTime + "-04:00[America/New_York]").toInstant().toString()
    Table allSnaps = db.liveTable("BookStates", "FlatBook")
            .where("Date = `${tradeDate}`", "SnapTime <= '${resumeTime}'", "!isNull(SnapTime)")
            .sort("SnapTime")

    def lastKey = allSnaps.getRowSet().lastRowKey()
    def lastSnapTime = allSnaps.getColumnSource("SnapTime").get(lastKey).toString()
    def lastTickTime = allSnaps.getColumnSource("LastTickTime").get(lastKey).toString()

    Table latestSnap = allSnaps.where("SnapTime = '${lastSnapTime}'")
    Table sourceSince = source.where("Date = `${tradeDate}`", "${TIME_COL} > '${lastTickTime}'")

    return [latestSnap, sourceSince]
}


// Note that I'm assuming timezone of ET in prepareResume
tradeTime = //set to something like "17:22:35.442"
source = eventsStatic
tradeDate = today()


(oldBookSnap, newSource) = prepareResume(tradeDate, tradeTime, source)


// Drop the cols that are not part of the book. Could do in the above function, but its good info for debugging
oldBook = oldBookSnap.dropColumns("Date", "SnapTime", "LastTickTime")


// ====== RESUME ======
""" 
Create a new book builder using the latest book snap and the new source data
"""
resumedBook = PriceBook.build(newSource, oldBook, *defaultCols, *passCols)


// ====== VERIFY (optional) ======
""" 
Check that the final book is the same as if you ran all the data through the book builder
"""
// compare to expected results
expectedBook = PriceBook.build(source, null, *defaultCols, *passCols)

