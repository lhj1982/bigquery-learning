streaming: Pub/Sub → Dataflow → BigQuery

Create a topic

gcloud pubsub topics create order-events

create a subscription

gcloud pubsub subscriptions create order-events-sub \
  --topic=order-events

Create another bigquery table
```
bq mk \
  --table \
  --time_partitioning_field event_timestamp \
  --time_partitioning_type DAY \
  --clustering_fields customer_id \
  "$PROJECT_ID:$DATASET.order_events" \
  'order_id:STRING,customer_id:STRING,event_timestamp:TIMESTAMP,amount:FLOAT,status:STRING'
```

Run stream order

```
python stream_orders.py \
  --subscription "projects/$PROJECT_ID/subscriptions/order-events-sub" \
  --output_table "$PROJECT_ID:$DATASET.order_events" \
  --runner DataflowRunner \
  --project $PROJECT_ID \
  --region $REGION \
  --temp_location gs://$BUCKET/tmp \
  --streaming
```

Publish an event
```
gcloud pubsub topics publish order-events \
  --message='{
    "order_id":"o100",
    "customer_id":"c10",
    "event_timestamp":"2026-10-07T09:30:00Z",
    "amount":199.50,
    "status":"PAID"
  }'
```