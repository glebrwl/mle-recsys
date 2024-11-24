from recommendations import Recommendations

rec_store = Recommendations()

# rec_store.load(
#     type="personal",
#     path='./top_recs.parquet',
#     columns=["user_id", "item_id", "rank"],
# )
rec_store.load(
    type="default",
    path='./top_recs.parquet',
    columns=["item_id", "rank"],
)

rec_store.get(user_id=100, k=5)