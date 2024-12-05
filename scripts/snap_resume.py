from deephaven.time import dh_today, dh_now, to_j_instant
from deephaven import parquet as dhpq
from deephaven import new_table, time_table, TableReplayer, merge, read_csv
from deephaven.table_listener import listen
from deephaven.column import datetime_col
from deephaven.numpy import to_numpy
from deephaven.execution_context import get_exec_ctx
from deephaven_enterprise.notebook import meta_import
from deephaven_enterprise import system_table_logger as stl

meta_import(db, "notebook")
from notebook.book_utils import prepare_data, build_book_with_snap


#--- Prepare some fake data ---#
trade_date = dh_today()
start_time = dh_now()
split_time = start_time.plusSeconds(30*3) #1.5 minutes
order_sample = prepare_data(start_time).update_view(f"Date = `{trade_date}`")

old_data = order_sample.where(f"EPOCH_TS < split_time")
rp = TableReplayer(start_time, split_time)
ticking_data = rp.add_table(order_sample, "EPOCH_TS")
rp.start()


#--- Run the book while saving snapshots ---#
curr_book = build_book_with_snap(ticking_data,\
            snapshot = None,\
            book_depth = 3).last_by("SYMB")


# Save snaps with listener. 
# (10 seconds just to get some quick snaps. In practice, should be closer to ~5 minutes)
snap_freq = "PT10S"
ctx = get_exec_ctx()

trigger = (
    time_table(snap_freq, None)
    .rename_columns(["SnapTime = Timestamp"])
)

# Listener function triggered by the time_table
def log_table(update, is_replay):
    with ctx:
        # Get the snap time and last timestamp from the source data
        snap_time = dh_now()
        last_tick_time = to_j_instant(to_numpy(ticking_data.tail(1), cols=["EPOCH_TS"]).flatten()[0])
        to_append = curr_book.update(["SnapTime = snap_time", "LastTickTime = last_tick_time"]).snapshot()
        stl.log_table("BookStates", "ExBook", to_append, columnPartition=dh_today(), \
            codecs={"Bid_Price" : stl.double_array_codec(), \
                "Bid_Timestamp" : stl.long_array_codec(), \
                "Bid_Size" : stl.int_array_codec(), \
                "Ask_Price" : stl.double_array_codec(), \
                "Ask_Timestamp" : stl.long_array_codec(), \
                "Ask_Size" : stl.int_array_codec()})

handle = listen(trigger, log_table)

# Remove handle after a few minutes. 
# Optionally view all snapshots.
handle.stop()
book_snapshots = db.live_table("BookStates", "ExBook").where("Date = today()")


#--- Resume from a certain time (adjust trade_time) ---# 
trade_time = "11:23:55"
source = order_sample

def prepare_resume(trade_date, trade_time, source):
    # Get first snapshot right before the given time
    resume_time = to_j_instant(trade_date + "T" + trade_time + " ET")
    latest_snap = db.live_table("BookStates", "ExBook").where([f"Date = `{trade_date}`", "SnapTime <= resume_time"])\
        .sort("SnapTime")\
        .last_by("SYMB").snapshot()

    # Get the actual last tick time so we can filter the source data from then, on
    last_tick_time = to_j_instant(to_numpy(latest_snap.tail(1), cols=["LastTickTime"]).flatten()[0])
    source_since = source.where([f"Date = `{trade_date}`", "EPOCH_TS > last_tick_time"])

    return latest_snap, source_since

old_book, new_source = prepare_resume(trade_date, trade_time, source)

# Make new book starting with old one
book = build_book_with_snap(source=new_source,\
    snapshot = old_book,\
    book_depth = 3).last_by("SYMB")


