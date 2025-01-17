from deephaven.parquet import read
from deephaven import TableReplayer, merge, read_csv
from deephaven.time import dh_now, to_j_instant
from datetime import datetime, timezone, timedelta
from deephaven.numpy import to_numpy
from deephaven import parquet as dhpq
import zoneinfo


from typing import Union, List
import jpy
from deephaven.table import Table

_J_BookBuilder = jpy.get_type("io.deephaven.book.PriceBook")

def build_book(source: Table,
                ord_id_col: str = "ORD_ID",
                prev_ord_id_col: str = "PREV_ORD_ID",
                sym_col: str = "SYMB",
               timestamp_col: str = "EPOCH_TS",
               price_col: str = "PRC",
               size_col: str = "QTY",
               exec_size_col: str = "EXEC_QTY",
               side_col: str = "SIDE",
               op_col: str = "EVT_ID"):


    return Table(_J_BookBuilder.build(source.j_object, ord_id_col, prev_ord_id_col, sym_col, timestamp_col, price_col, size_col, exec_size_col, side_col, op_col))


def prepare_data(start_time):
    # OAK: new order ack;
    # CRAK: cancel replace ack;
    # CC: order cancel;
    # INF: internal fill;
    # AWF: away market fill.
    EVT_map = {"Order Ack": 1, "Cancel Replace Ack" : 2, "Cancel Order": 3, "Internal Fill": 4, "Away Market Fill": 5}

    order_sample = read_csv("/data/order_sample.csv")\
        .view(["EVT_TYP", "FIRM= EPOCH_TS % 2 = 0 ? `Traders Inc` : `Big Bank Inc`", "SYMB", "EPOCH_TS", "ORD_ID=ORD_ID_PUB", "ORD_QTY=QTY", "EXEC_QTY=(int) null", "CXL_QTY=(int) null", "PRC", "SIDE"])\
        .sort("EPOCH_TS")


    # Align timestamps to now
    now_time = start_time
    order_sample_start = to_numpy(order_sample, cols=["EPOCH_TS"]).flatten()[0]
    order_sample = order_sample.update_view(["NowTime = now_time.getEpochSecond()*SECOND + now_time.getNano()",
            "Diff = NowTime - (long) order_sample_start",
            f"EPOCH_TS_Nano = EPOCH_TS+Diff",
            f"EPOCH_TS = Instant.ofEpochSecond((long) ((EPOCH_TS_Nano)/SECOND), (EPOCH_TS_Nano) % SECOND)", 
            # We are ignoring the actual book build for now so make everything 1...
            "EVT_ID = 1"])

    return order_sample
