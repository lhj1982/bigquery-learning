import argparse
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp


def parse_event(message):
    event = json.loads(message.decode("utf-8"))

    return {
        "order_id": event["order_id"],
        "customer_id": event["customer_id"],
        "event_timestamp": event["event_timestamp"],
        "amount": float(event["amount"]),
        "status": event["status"],
    }


def event_timestamp(event):
    dt = datetime.fromisoformat(
        event["event_timestamp"].replace("Z", "+00:00")
    )

    return Timestamp.from_utc_datetime(dt)


class OrderStatsCombineFn(beam.CombineFn):

    def create_accumulator(self):
        # count, total_amount
        return 0, 0.0

    def add_input(self, accumulator, value):
        count, total_amount = accumulator

        return (
            count + 1,
            total_amount + value,
        )

    def merge_accumulators(self, accumulators):
        counts = 0
        total_amount = 0.0

        for count, amount in accumulators:
            counts += count
            total_amount += amount

        return counts, total_amount

    def extract_output(self, accumulator):
        count, total_amount = accumulator

        return {
            "order_count": count,
            "total_amount": total_amount,
        }


class FormatResult(beam.DoFn):

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
    ):
        customer_id, stats = element

        yield {
            "customer_id": customer_id,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "order_count": stats["order_count"],
            "total_amount": stats["total_amount"],
        }


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--subscription",
        required=True,
    )

    parser.add_argument(
        "--output_table",
        required=True,
    )

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        save_main_session=True,
        streaming=True,
    )

    with beam.Pipeline(options=options) as pipeline:

        events = (
            pipeline
            | "Read PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=args.subscription
            )

            | "Parse JSON"
            >> beam.Map(parse_event)

            # Tell Beam to use event_timestamp
            # rather than processing time
            | "Assign Event Time"
            >> beam.Map(
                lambda event:
                    beam.window.TimestampedValue(
                        event,
                        event_timestamp(event),
                    )
            )
        )

        aggregated = (
            events

            | "Only Paid Orders"
            >> beam.Filter(
                lambda event:
                    event["status"] == "PAID"
            )

            | "5 Minute Windows"
            >> beam.WindowInto(
                FixedWindows(5 * 60)
            )

            | "Key By Customer"
            >> beam.Map(
                lambda event: (
                    event["customer_id"],
                    event["amount"],
                )
            )

            | "Aggregate Per Customer"
            >> beam.CombinePerKey(
                OrderStatsCombineFn()
            )

            | "Format Results"
            >> beam.ParDo(
                FormatResult()
            )
        )

        (
            aggregated
            | "Write Aggregates To BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=(
                    "customer_id:STRING,"
                    "window_start:TIMESTAMP,"
                    "window_end:TIMESTAMP,"
                    "order_count:INTEGER,"
                    "total_amount:FLOAT"
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
                method=(
                    beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
                ),
            )
        )


if __name__ == "__main__":
    run()