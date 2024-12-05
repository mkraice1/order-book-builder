from deephaven.time import dh_today, dh_now, to_j_instant
from deephaven_enterprise.notebook import meta_import

meta_import(db, "notebook")
from notebook.book_utils import prepare_data, build_book_with_snap

#--- Prepare some fake data ---#
trade_date = dh_today()
start_time = dh_now()
split_time = start_time.plusSeconds(30*3) #1.5 minutes
order_sample = prepare_data(start_time).update_view(f"Date = `{trade_date}`")


#--- Run the book ---#
curr_book = build_book_with_snap(order_sample,\
            snapshot = None,\
            book_depth = 3,
            group_cols=["FIRM", "SYMB"]).last_by(["FIRM", "SYMB"])

