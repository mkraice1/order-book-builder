'''
Questions
Contract ord limit prc vs not-contract prc in fills
Which order id?
ORD_ID_PUB_PAR? ORD_ID_PUB?
1 == BUY?
2 == SELL?

SNAPSHOT
'''

from deephaven.parquet import read
from deephaven import TableReplayer, merge, read_csv
import bookbuilder

# OAK: new order ack; 
# CRAK: cancel replace ack; 
# CC: order cancel;
# INF: internal fill; 
# AWF: away market fill.

EVT_map = {"Order Ack": 1, "Cancel Replace Ack" : 2, "Cancel Order": 3, "Internal Fill": 4, "Away Market Fill": 5}#2}

#  Consolidate everything into a single table
order_sample_raw = read_csv("/data/order_sample.csv")
fill_sample_raw = read_csv("/data/fill_sample.csv")
cancel_sample_raw = read_csv("/data/cancel_sample.csv")

order_sample = read_csv("/data/order_sample.csv").view(["EVT_TYP", "SYMB", "EPOCH_TS", "ORD_ID=CLIENT_ORD_ID", "ORD_QTY=QTY", "EXEC_QTY=(int) null", "CXL_QTY=(int) null", "PRC", "SIDE"])
fill_sample = read_csv("/data/fill_sample.csv").view(["EVT_TYP", "SYMB", "EPOCH_TS", "ORD_ID=ORD_ID_PUB", "ORD_QTY", "EXEC_QTY", "CXL_QTY=(int) null", "PRC=ORD_LIMIT_PRC", "SIDE=ORD_SIDE"])
cancel_sample = read_csv("/data/cancel_sample.csv").view(["EVT_TYP", "SYMB", "EPOCH_TS", "ORD_ID=(long) ORD_CLIENT_ORD_ID", "ORD_QTY","EXEC_QTY=(int) null", "CXL_QTY", "PRC=ORD_LIMIT_PRC", "SIDE=ORD_SIDE"])

all_events = merge([order_sample, fill_sample, cancel_sample])\
    .update_view(["EPOCH_TS = Instant.ofEpochSecond((long) (EPOCH_TS/SECOND), EPOCH_TS % SECOND)", 
        "EVT_ID = 1"])\
    .sort("EPOCH_TS")
    #EVT_map.containsKey(EVT_TYP) ? (int) EVT_map[EVT_TYP] : null"])\

rp = TableReplayer("2024-10-10T02:30:00 ET", "2024-10-25T02:40:00 ET")
ticking_data = rp.add_table(all_events, "EPOCH_TS")
rp.start()

#  Try with just orders...
order_sample = order_sample.update_view(["EPOCH_TS = Instant.ofEpochSecond((long) (EPOCH_TS/SECOND), EPOCH_TS % SECOND)", 
        "EVT_ID = 1"])

rp = TableReplayer("2024-10-10T02:30:00 ET", "2024-10-25T02:40:00 ET")
ticking_data = rp.add_table(order_sample, "EPOCH_TS")
rp.start()


book = bookbuilder.build_book(source=order_sample,\
            book_depth = 2,\
            timestamp_col = "EPOCH_TS",\
            size_col = "ORD_QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"]).last_by("SYMB")









            



