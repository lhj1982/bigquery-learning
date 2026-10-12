import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def parse_event(message):
    event = json.loads(message.decode("utf-8"))

    return {
        "order_id": event["order_id"],
        "customer_id": event["customer_id"],
        "event_timestamp": event["event_timestamp"],
        "amount": float(event["amount"]),
        "status": event["status"],
    }


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument("--subscription", required=True)
    parser.add_argument("--output_table", required=True)

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        save_main_session=True,
        streaming=True
    )

    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline
            | "Read PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=args.subscription
            )

            | "Parse JSON"
            >> beam.Map(parse_event)

            | "Write BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=(
                    "order_id:STRING,"
                    "customer_id:STRING,"
                    "event_timestamp:TIMESTAMP,"
                    "amount:FLOAT,"
                    "status:STRING"
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )


if __name__ == "__main__":
    run()