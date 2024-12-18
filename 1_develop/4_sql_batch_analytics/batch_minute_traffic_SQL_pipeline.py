import argparse
import time
import logging
import json
import typing
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.sql import SqlTransform
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

class CommonLog(typing.NamedTuple):
    ip: str
    user_id: str
    lat: float
    lng: float
    ts: str
    http_request: str
    http_response: int
    num_bytes: int
    user_agent: str

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)

def parse_json(element):
    row = json.loads(element)
    row['ts'] = row['timestamp']
    row.pop('timestamp')
    return CommonLog(**row)

def format_timestamp(element):
    ts = datetime.strptime(element.ts[:-8], "%Y-%m-%dT%H:%M:%S")
    ts = datetime.strftime(ts, "%Y-%m-%d %H:%M:%S")
    temp_dict = element._asdict()
    temp_dict['ts'] = ts
    return CommonLog(**temp_dict)

def to_dict(row):
    return {'page_views' : row.page_views,
            'start_time' : row.start_time}

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--inputPath', required=True, help='Path to events.json')
    parser.add_argument('--tableName', required=True, help='BigQuery table name')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts, save_main_session=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('batch-minute-traffic-pipeline-sql'
                                                                   ,time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.inputPath
    table_name = opts.tableName

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "page_views",
                "type": "INTEGER"
            },
            {
                "name": "start_time",
                "type": "STRING"
            },

        ]
    }

    query = '''
        SELECT
            COUNT(*) AS page_views,
            STRING(window_start) AS start_time
        FROM
            TUMBLE(
                (SELECT TIMESTAMP(ts) AS ts FROM PCOLLECTION),
                DESCRIPTOR(ts),
                'INTERVAL 1 MINUTE')
        GROUP BY window_start
    '''

    # Create the pipeline
    p = beam.Pipeline(options=options)

    (p | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)
       | 'ParseJson' >> beam.Map(parse_json).with_output_types(CommonLog)
       | 'FormatTimestamp' >> beam.Map(format_timestamp).with_output_types(CommonLog)
       | "CountPerMinute" >> SqlTransform(query, dialect='zetasql')
       | "ConvertToDict" >> beam.Map(to_dict)
       | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()

# export PROJECT_ID=$(gcloud config get-value project)
# export REGION=us-central1
# export BUCKET=gs://${PROJECT_ID}
# export PIPELINE_FOLDER=${BUCKET}
# export RUNNER=DataflowRunner
# export INPUT_PATH=${PIPELINE_FOLDER}/events.json
# export TABLE_NAME=${PROJECT_ID}:logs.minute_traffic_sql

# python3 batch_minute_traffic_SQL_pipeline.py \
# --project=${PROJECT_ID} \
# --region=${REGION} \
# --stagingLocation=${PIPELINE_FOLDER}/staging \
# --tempLocation=${PIPELINE_FOLDER}/temp \
# --runner=${RUNNER} \
# --inputPath=${INPUT_PATH} \
# --tableName=${TABLE_NAME} \
# --experiments=use_runner_v2