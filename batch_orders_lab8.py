import argparse
import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ParseOrder(beam.DoFn):
    BAD_RECORD = "bad_record"

    def process(self, line):
        try:
            values = next(csv.reader([line]))

            if len(values) != 5:
                raise ValueError(
                    f"Expected 5 columns but got {len(values)}"
                )

            order = {
                "order_id": values[0],
                "customer_id": values[1],
                "event_date": values[2],
                "amount": float(values[3]),
                "status": values[4],
            }

            if order["amount"] < 0:
                raise ValueError("Amount cannot be negative")

            yield order

        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                self.BAD_RECORD,
                {
                    "raw_record": line,
                    "error": str(e),
                },
            )


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--error_table", required=True)

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        save_main_session=True,
    )

    with beam.Pipeline(options=options) as pipeline:

        parsed = (
            pipeline
            | "Read CSV"
            >> beam.io.ReadFromText(
                args.input,
                skip_header_lines=1,
            )
            | "Parse Orders"
            >> beam.ParDo(ParseOrder()).with_outputs(
                ParseOrder.BAD_RECORD,
                main="good",
            )
        )

        good_orders = parsed.good
        bad_orders = parsed.bad_record

        (
            good_orders
            | "Write Good Orders to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=(
                    "order_id:STRING,"
                    "customer_id:STRING,"
                    "event_date:DATE,"
                    "amount:FLOAT,"
                    "status:STRING"
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
            )
        )

        (
            bad_orders
            | "Write Bad Orders to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.error_table,
                schema=(
                    "raw_record:STRING,"
                    "error:STRING"
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
            )
        )


if __name__ == "__main__":
    run()