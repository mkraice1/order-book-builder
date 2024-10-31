
from deephaven.parquet import read
from deephaven import TableReplayer, merge, read_csv
import bookbuilder

# OAK: new order ack; 
# CRAK: cancel replace ack; 
# CC: order cancel;
# INF: internal fill; 
# AWF: away market fill.


EVT_map = {"Order Ack": 1, "Cancel Replace Ack" : 2, "Cancel Order": 3, "Internal Fill": 4, "Away Market Fill": 5}#2}

order_sample = read_csv("/data/order_sample.csv").view(["EVT_TYP", "SYMB", "EPOCH_TS", "ORD_ID=CLIENT_ORD_ID", "ORD_QTY=QTY", "EXEC_QTY=(int) null", "CXL_QTY=(int) null", "PRC", "SIDE"])

#  Try with just orders...
order_sample = order_sample.update_view(["EPOCH_TS = Instant.ofEpochSecond((long) (EPOCH_TS/SECOND), EPOCH_TS % SECOND)", 
        "EVT_ID = 1"])

orderrp = TableReplayer("2024-10-10T02:29:55 ET", "2024-10-25T02:40:00 ET")
order_ticking_data = orderrp.add_table(order_sample, "EPOCH_TS")
orderrp.start()

# Get some old book
static_data_ = read("/data/Quotes.parquet")
rp_ = TableReplayer("2017-08-26T09:30:00 ET", "2017-08-26T23:59:59 ET")
ticking_data_ = rp_.add_table(static_data_, "Timestamp")
rp_.start()
old_book = bookbuilder.build_book(ticking_data_).rename_columns("SYMB=Sym").last_by("SYMB")
old_book = old_book.snapshot()


# Make new book starting with old one
book = bookbuilder.build_book_with_snap(source=order_ticking_data,\
            snapshot = old_book,\
            book_depth = 2,\
            timestamp_col = "EPOCH_TS",\
            size_col = "ORD_QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"]).last_by("SYMB")



