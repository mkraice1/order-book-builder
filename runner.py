# ====== IMPORTS & SETUP ======
""" 
This section handles all necessary imports and function declarations.
"""
from deephaven.time import dh_today, dh_now, to_j_instant
from deephaven import parquet as dhpq
from deephaven import new_table, time_table, TableReplayer, merge, read_csv
from deephaven.table_listener import listen
from deephaven.column import datetime_col
from deephaven.numpy import to_numpy
from deephaven.execution_context import get_exec_ctx
from deephaven_enterprise.notebook import meta_import
from deephaven_enterprise import system_table_logger as stl
from deephaven.update_graph import shared_lock, UpdateGraph


import time
import jpy
from datetime import datetime
from threading import Timer

from typing import Union, List
from deephaven.table import Table
import threading
import time


_J_BookBuilder = jpy.get_type("p.deephaven.book.PriceBook")


def build_book(source: Table,
                snapshot: Table = None,
                ord_id_col: str = "ORD_ID",
                prev_ord_id_col: str = "PREV_ORD_ID",
               size_col: str = "QTY",
               exec_size_col: str = "EXEC_QTY",
               op_col: str = "EVT_ID",
               passthrough_cols: List[str] = []):

    if snapshot is not None:
        snapshot = snapshot.j_object

    return Table(_J_BookBuilder.build(source.j_object, snapshot, ord_id_col, prev_ord_id_col, size_col, exec_size_col, op_col, passthrough_cols))


def prepare_data(start_time, events_stream):
        # Align timestamps to start_time
        now_time = start_time
        now_nanos = start_time.getEpochSecond()*1000000000 + start_time.getNano()
        order_sample_start = to_numpy(events_stream, cols=["EPOCH_TS"]).flatten()[0].astype('int64')

        events_stream = events_stream.sort("EPOCH_TS").update_view("FakeTime = Instant.ofEpochSecond( (long) (now_nanos / 1_000_000_000), (now_nanos % 1_000_000_000)).plusNanos(ii*2000)")
        return events_stream



# ====== PREPARE DATA ======
"""
Load and transform the csv data. Still static.
I am using 'FakeTime' so I have full control over the speed of ingestion.
It will currently spit out .5M rows/second
"""
if 'events' not in globals():
    events = read_csv("/tmp/data/event.csv")

# Align data with a made up timestamp "FakeTime"
trade_date = dh_today()
start_time = dh_now()
end_time = start_time.plusSeconds(60*5)
events_static = prepare_data(start_time, events)\
    .update_view([f"Date = `{trade_date}`", "MyChar = (char) 'A'"])


# ====== STREAM + PROCESS DATA WHILE SNAPSHOTTING ======
"""
Here we start streaming the prepared data, run it through the book builder, and save snapshots along the way
Snapshotting could be done in a separate worker if it is interfering with performance too much.
It performs fine, so I have not done that.
"""
rp = TableReplayer(start_time, end_time)
events_ticking = rp.add_table(events_static, "FakeTime")
rp.start()

pass_cols = ["SYMB", "SIDE", "PRC", "EPOCH_TS"]
curr_book = build_book(events_ticking,\
            snapshot = None, passthrough_cols=pass_cols)

# Save snaps with thread.
# (10 seconds just to get some quick snaps. In practice you might want it more spaced out)
snap_freq = 10
ctx = get_exec_ctx()

# This time column is used to choose the exact moment to resume the source data from
# In practice, this should probably be "EPOCH_TS", but I'm using "FakeTime" for convinience
TIME_COL = "FakeTime"

def log_table():
    global timer
    print(f"Task executed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    task_start = datetime.now()

    with ctx:
        with shared_lock(ctx):
            # Get the snap time and last timestamp from the source data
            snap_time = dh_now()
            last_tick_time = to_j_instant(to_numpy(events_ticking.tail(1), cols=[TIME_COL]).flatten()[0])
            to_append = curr_book.update(["SnapTime = snap_time", "LastTickTime = last_tick_time"])#.snapshot()
            stl.log_table("BookStates", "FlatBook", to_append, columnPartition=dh_today())

    next_snap = max(snap_freq - (datetime.now() - task_start).total_seconds(), 1)
    timer = threading.Timer(next_snap, log_table)
    timer.start()

# Start the first task
timer = threading.Timer(1, log_table)
timer.start()


# ====== REMOVE HANDLE (optional) ======
"""
After ~30 seconds or so, you may want to cancel the timer. Or you can let it run until the end of the data.
You might also want to view the snapshots that were taken.
"""
timer.cancel()
book_snapshots = db.live_table("BookStates", "FlatBook").where("Date = today()")
snap_time = book_snapshots.select_distinct(["SnapTime", "LastTickTime"])


# ====== PREPARE RESUME ======
"""
Specify a time and date you want to resume from.
Get the book snapshot right before that time and filter the source
    to after the last tick time that the book had processed.
"""

# Note that I'm assuming timezone of ET in prepare_resume
trade_time = #"13:54:27.297"
source = events_static
trade_date = dh_today()

def prepare_resume(trade_date, trade_time, source):
    # Get first snapshot right before the given time
    resume_time = to_j_instant(trade_date + "T" + trade_time + " ET")
    all_snaps = db.live_table("BookStates", "FlatBook")\
        .where([f"Date = `{trade_date}`", "SnapTime <= resume_time"])\
        .sort("SnapTime")

    last_snap_time, last_tick_time = [to_j_instant(item) for item in to_numpy(all_snaps.tail(1), cols=["SnapTime", "LastTickTime"]).flatten()]

    latest_snap = all_snaps.where("SnapTime = last_snap_time")#.drop_columns(["Date", "SnapTime", "LastTickTime"])
    source_since = source.where([f"Date = `{trade_date}`", f"{TIME_COL} > last_tick_time"])

    return latest_snap, source_since

old_book_snap, new_source = prepare_resume(trade_date, trade_time, source)

# Drop the cols that are not part of the book. Could do in the above function, but its good info for debugging
old_book = old_book_snap.drop_columns(["Date", "SnapTime", "LastTickTime"])



# ====== RESUME ======
"""
Create a new book builder using the latest book snap and the new source data
"""
final_book = build_book(source=new_source,\
    snapshot = old_book,\
    passthrough_cols=pass_cols)


# ====== VERIFY (optional) ======
""" 
Check that the final book is the same as if you ran all the data through the book builder
"""
# compare to expected results
expected_book = build_book(source=source,\
    snapshot = None,\
    passthrough_cols=pass_cols)
