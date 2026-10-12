Introduce bad data

add a malformed data

```
o6,c4,2026-10-03,INVALID,PAID
```


```
python batch_orders_lab8.py \
  --input data/orders_lab8.csv \
  --output_table "$PROJECT_ID:$DATASET.orders" \
  --error_table "$PROJECT_ID:$DATASET.order_errors" \
  --runner DirectRunner \
  --temp_location gs://$BUCKET/tmp
```