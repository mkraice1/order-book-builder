from deephaven.parquet import read
from deephaven import TableReplayer
import bookbuilder

static_data = read("/data/Quotes.parquet")
rp = TableReplayer("2017-08-25T09:30:00 ET", "2017-08-25T23:59:59 ET")
ticking_data = rp.add_table(static_data, "Timestamp")
rp.start()

book = bookbuilder.build_book(ticking_data) \
                  .last_by("Key")