Run it locally

Run Beam locally
```
python batch_orders.py \
  --input data/orders.csv \
  --output_table "$PROJECT_ID:$DATASET.orders" \
  --runner DirectRunner \
  --temp_location gs://$BUCKET/tmp
```

Check

```
bq query --use_legacy_sql=false \
"SELECT * FROM \`$PROJECT_ID.$DATASET.orders\` ORDER BY event_date"
```