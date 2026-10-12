Run on dataflow

python batch_orders.py \
  --input gs://$BUCKET/input/orders.csv \
  --output_table "$PROJECT_ID:$DATASET.orders" \
  --runner DataflowRunner \
  --project $PROJECT_ID \
  --region $REGION \
  --temp_location gs://$BUCKET/tmp \
  --staging_location gs://$BUCKET/staging