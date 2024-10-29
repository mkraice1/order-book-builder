from deephaven.parquet import read
from deephaven import TableReplayer, merge
import bookbuilder

# OAK: new order ack; CRAK: cancel replace ack; CC: order cancel;
# INF: internal fill; AWF: away market fill.

EVT_map = {"Order Ack": 1, "Cancel Replace Ack" : 1}#2}
static_data_ = read("/data/Quotes.parquet")

static_data = order_sample.update("EPOCH_TS = Instant.ofEpochSecond((long) (EPOCH_TS/SECOND), EPOCH_TS % SECOND)")\
.update_view("EVT_ID = EVT_map.containsKey(EVT_TYP) ? (int) EVT_map[EVT_TYP] : null")


rp = TableReplayer("2024-10-10T02:30:00 ET", "2024-10-25T02:40:00 ET")
ticking_data = rp.add_table(static_data, "EPOCH_TS")
rp.start()

book = bookbuilder.build_book(source=ticking_data,\
            book_depth = 1,\
            timestamp_col = "EPOCH_TS",\
            size_col = "QTY",\
            side_col = "SIDE",\
            op_col = "EVT_ID",\
            price_col = "PRC",\
            group_cols = ["SYMB"])
