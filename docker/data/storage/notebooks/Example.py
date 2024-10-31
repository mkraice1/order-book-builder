from deephaven.parquet import read
from deephaven import TableReplayer
import bookbuilder

static_data_ = read("/data/Quotes.parquet")
rp_ = TableReplayer("2017-08-25T09:30:00 ET", "2017-08-25T23:59:59 ET")
ticking_data_ = rp_.add_table(static_data_, "Timestamp")
rp_.start()

book_ = bookbuilder.build_book(ticking_data_).last_by("Sym").rename_columns("SYMB=Sym")