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

#  Try with just orders... make everything a 1
order_sample = order_sample.update_view(["EPOCH_TS = Instant.ofEpochSecond((long) (EPOCH_TS/SECOND), EPOCH_TS % SECOND)", 
        "EVT_ID = 1"])

# Get some old book
old_data = order_sample.where("EPOCH_TS < '2024-10-10T02:30:01.007 ET'")

old_book = bookbuilder.build_book(old_data,\
            book_depth = 3,\
            timestamp_col = "EPOCH_TS",\
            size_col = "ORD_QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"])
old_book = old_book.snapshot()


new_data = order_sample.where("EPOCH_TS >= '2024-10-10T02:30:01.007 ET'")

# Make new book starting with old one
book = bookbuilder.build_book_with_snap(source=new_data,\
            snapshot = old_book,\
            book_depth = 3,\
            timestamp_col = "EPOCH_TS",\
            size_col = "ORD_QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"]).last_by("SYMB")


