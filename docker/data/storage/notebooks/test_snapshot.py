from deephaven.parquet import read
from deephaven import TableReplayer, merge, read_csv
from deephaven.time import dh_now, to_j_instant
from datetime import datetime, timezone, timedelta
from deephaven.numpy import to_numpy
from deephaven import parquet as dhpq
import zoneinfo
import bookbuilder

# OAK: new order ack; 
# CRAK: cancel replace ack; 
# CC: order cancel;
# INF: internal fill; 
# AWF: away market fill.

EVT_map = {"Order Ack": 1, "Cancel Replace Ack" : 2, "Cancel Order": 3, "Internal Fill": 4, "Away Market Fill": 5}
order_sample = read_csv("/data/order_sample.csv").view(["EVT_TYP", "SYMB", "EPOCH_TS", "ORD_ID=CLIENT_ORD_ID", "ORD_QTY=QTY", "EXEC_QTY=(int) null", "CXL_QTY=(int) null", "PRC", "SIDE"])\
        .sort("EPOCH_TS")

# Align timestamps to now
now_time = dh_now()
order_sample_start = to_numpy(order_sample, cols=["EPOCH_TS"]).flatten()[0]
order_sample = order_sample.update_view(["NowTime = now_time.getEpochSecond()*SECOND + now_time.getNano()",
        "Diff = NowTime - (long) order_sample_start",
        f"EPOCH_TS_Nano = EPOCH_TS+Diff",
        f"EPOCH_TS = Instant.ofEpochSecond((long) ((EPOCH_TS_Nano)/SECOND), (EPOCH_TS_Nano) % SECOND)", 
        # We are ignoring the actual book build for now so make everything 1...
        "EVT_ID = 1"])


# Get some old book. Choose where to split the data...
split_time = to_j_instant(to_numpy(order_sample, cols=["EPOCH_TS"]).flatten()[5])
old_data = order_sample.where(f"EPOCH_TS < split_time")

old_book = bookbuilder.build_book_with_snap(old_data,\
            snapshot = None,\
            book_depth = 3,\
            timestamp_col = "EPOCH_TS",\
            size_col = "ORD_QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"]).last_by("SYMB")
old_book = old_book.snapshot()

# Save the book for later
dhpq.write(old_book, "/tmp/old_book.parquet")



# Start up the rest of the data
# And run for 10 minutes
new_data = order_sample.where("EPOCH_TS >= split_time")
rp = TableReplayer(now_time.minusNanos(5*1000000000), now_time.plusNanos(10*60*1000000000))
ticking_data = rp.add_table(new_data, "EPOCH_TS")
rp.start()

# Load old book (or just grab the variable if running in one sequence)
old_book_snapshot = dhpq.read("/tmp/old_book.parquet")

# Make new book starting with old one
book = bookbuilder.build_book_with_snap(source=ticking_data,\
            snapshot = old_book_snapshot,\
            book_depth = 3,\
            timestamp_col = "EPOCH_TS",\
            size_col = "ORD_QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"]).last_by("SYMB")


