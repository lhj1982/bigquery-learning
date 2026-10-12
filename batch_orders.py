import argparse
import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def parse_csv(line: str):
    values = next(csv.reader([line]))

    return {
        "order_id": values[0],
        "customer_id": values[1],
        "event_date": values[2],
        "amount": float(values[3]),
        "status": values[4],
    }


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output_table", required=True)

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline
            | "Read CSV"
            >> beam.io.ReadFromText(
                args.input,
                skip_header_lines=1
            )

            | "Parse CSV"
            >> beam.Map(parse_csv)

            | "Only positive amounts"
            >> beam.Filter(
                lambda row: row["amount"] >= 0
            )

            | "Write to BigQuery"
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
                )
            )
        )


if __name__ == "__main__":
    run()