5 mins window

Create table
```
bq mk \
  --table \
  --time_partitioning_field window_start \
  --time_partitioning_type DAY \
  --clustering_fields customer_id \
  "$PROJECT_ID:$DATASET.order_aggregates" \
  'customer_id:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,order_count:INTEGER,total_amount:FLOAT'
```

```
python stream_order_aggregation.py \
  --subscription "projects/$PROJECT_ID/subscriptions/order-events-sub" \
  --output_table "$PROJECT_ID:$DATASET.order_aggregates" \
  --runner DataflowRunner \
  --project $PROJECT_ID \
  --region $REGION \
  --temp_location gs://$BUCKET/tmp \
  --streaming
```

publish events

```
gcloud pubsub topics publish order-events \
  --message='{
    "order_id":"o201",
    "customer_id":"c10",
    "event_timestamp":"2026-10-11T14:01:00Z",
    "amount":100,
    "status":"PAID"
  }'

gcloud pubsub topics publish order-events \
  --message='{
    "order_id":"o202",
    "customer_id":"c10",
    "event_timestamp":"2026-10-11T14:02:00Z",
    "amount":150,
    "status":"PAID"
  }'

gcloud pubsub topics publish order-events \
  --message='{
    "order_id":"o203",
    "customer_id":"c10",
    "event_timestamp":"2026-10-11T14:04:30Z",
    "amount":200,
    "status":"PAID"
  }'
```