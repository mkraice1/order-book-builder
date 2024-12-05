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


def build_book_with_snap(source: Table,
               snapshot: Table,
               book_depth: int = 2,
               batch_timestamps: bool = False,
               timestamp_col: str = "EPOCH_TS",
               size_col: str = "ORD_QTY",
               side_col: str = "SIDE",
               op_col: str = "EVT_ID",
               price_col: str = "PRC",
               ord_id_col: str = "ORD_ID",
               group_cols: Union[str, List[str]] = ["SYMB"]):

    if snapshot is not None:
        snapshot = snapshot.j_object

    return Table(_J_BookBuilder.build(source.j_object, snapshot, book_depth, batch_timestamps, timestamp_col, size_col, side_col, op_col, price_col, ord_id_col, group_cols))


def prepare_data(start_time):
        # OAK: new order ack;
        # CRAK: cancel replace ack;
        # CC: order cancel;
        # INF: internal fill;
        # AWF: away market fill.
        EVT_map = {"Order Ack": 1, "Cancel Replace Ack" : 2, "Cancel Order": 3, "Internal Fill": 4, "Away Market Fill": 5}

        order_sample = read_csv("/tmp/data/order_sample.csv").view(["EVT_TYP", "FIRM= EPOCH_TS % 2 = 0 ? `Traders Inc` : `Big Bank Inc`", "SYMB", "EPOCH_TS", "ORD_ID=ORD_ID_PUB", "ORD_QTY=QTY", "EXEC_QTY=(int) null", "CXL_QTY=(int) null", "PRC", "SIDE"])\
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

