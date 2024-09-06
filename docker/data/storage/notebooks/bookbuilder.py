from typing import Union, List

import jpy
from deephaven.table import Table

_J_BookBuilder = jpy.get_type("io.deephaven.book.PriceBook")

def build_book(source: Table,
               book_depth: int = 5,
               batch_timestamps: bool = False,
               timestamp_col: str = "Timestamp",
               size_col: str = "Size",
               side_col: str = "Side",
               op_col: str = "Op",
               price_col: str = "Price",
               group_cols: Union[str, List[str]] = ["Sym"]):
    return Table(_J_BookBuilder.build(source.j_object, book_depth, batch_timestamps, timestamp_col, size_col, side_col, op_col, price_col, group_cols))