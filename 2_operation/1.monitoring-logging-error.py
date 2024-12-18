import argparse
import logging
import argparse, logging, os
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

class ReadGBK(beam.DoFn):

 def process(self, e):
   k, elems = e
   for v in elems:
     logging.info(f"the element is {v}")
     yield v


def run(argv=None):
   parser = argparse.ArgumentParser()
   parser.add_argument(
     '--output', dest='output', help='Output file to write results to.')
   known_args, pipeline_args = parser.parse_known_args(argv)
   read_query = """(
                 SELECT
                   version,
                   block_hash,
                   block_number
                 FROM
                   `bugquery-public-data.crypto_bitcoin.transactions`
                 WHERE
                   version = 1
                 LIMIT
                   1000000 )
               UNION ALL (
                 SELECT
                   version,
                   block_hash,
                   block_number
                 FROM
                   `bigquery-public-data.crypto_bitcoin.transactions`
                 WHERE
                   version = 2
                 LIMIT
                   1000 ) ;"""
   p = beam.Pipeline(options=PipelineOptions(pipeline_args))
   (p
   | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=read_query, use_standard_sql=True)
   | "Add Hotkey" >> beam.Map(lambda elem: (elem["version"], elem))
   | "Groupby" >> beam.GroupByKey()
   | 'Print' >>  beam.ParDo(ReadGBK())
   | 'Sink' >>  WriteToText(known_args.output))

   result = p.run()

if __name__ == '__main__':
 logger = logging.getLogger().setLevel(logging.INFO)
 run()


# # Attempt to launch the pipeline
# python3 my_pipeline.py \
#   --project=${PROJECT_ID} \
#   --region=us-central1 \
#   --tempLocation=gs://$PROJECT_ID/temp/ \
#   --runner=DataflowRunner

# # Launch the pipeline
# python3 my_pipeline.py \
#   --project=${PROJECT_ID} \
#   --region=us-central1 \
#   --output gs://$PROJECT_ID/results/prefix \
#   --tempLocation=gs://$PROJECT_ID/temp/ \
#   --max_num_workers=5 \
#   --runner=DataflowRunner

# # Launch the pipeline
# python3 my_pipeline.py \
#   --project=${PROJECT_ID} \
#   --region=us-central1 \
#   --output gs://$PROJECT_ID/results/prefix \
#   --tempLocation=gs://$PROJECT_ID/temp/ \
#   --max_num_workers=5 \
#   --runner=DataflowRunner
