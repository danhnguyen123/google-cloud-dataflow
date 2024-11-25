import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output table to write results to.')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
outputs_prefix = path_args.output

options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)

def remove_special_characters(row):    # oxjy167254jk,11-11-2020,8:11:21,854a854,chow m?ein:,65,cash,sadabahar,delivered,5,awesome experience
    import re
    cols = row.split(',')			# [(oxjy167254jk) (11-11-2020) (8:11:21) (854a854) (chow m?ein) (65) (cash) ....]
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col)
        ret = ret + clean_col + ','			# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,
    ret = ret[:-1]						# oxjy167254jk,11-11-2020,8:11:21,854A854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience
    return ret

cleaned_data = (
	p
	| beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
	| beam.Map(lambda row: row.lower())
	| beam.Map(remove_special_characters)
	| beam.Map(lambda row: row+',1')		# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,1
)

#BigQuery

client = bigquery.Client()

dataset_id = "danh-nguyen-403304.food_orders_dataset"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "US"
dataset.description = "dataset for food orders"

# dataset_ref = client.create_dataset(dataset, timeout = 30)

def to_json(csv_str):
    fields = csv_str.split(',')

    json_str = {"customer_id":fields[0],
                 "date": fields[1],
                 "timestamp": fields[2],
                 "order_id": fields[3],
                 "items": fields[4],
                 "amount": fields[5],
                 "mode": fields[6],
                 "restaurant": fields[7],
                 "status": fields[8],
                 "ratings": fields[9],
                 "feedback": fields[10],
                 "new_col": fields[11]
                 }

    return json_str

table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'

(cleaned_data
| 'cleaned_data to json' >> beam.Map(to_json)
| 'write to bigquery' >> beam.io.WriteToBigQuery(
outputs_prefix,
schema=table_schema,
create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
additional_bq_parameters={'timePartitioning' : {'type':'DAY'}}

)

)

from apache_beam.runners.runner import PipelineState
ret = p.run()
if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error Running beam pipeline')


# python3 14.Dataflow-Bigquery.py --input gs://dataflow-playground/food_daily.csv --output danh-nguyen-403304:food_orders_dataset.food_daily --runner DataflowRunner --project danh-nguyen-403304 --region us-central1 --temp_location gs://dataflow-playground/temp --no_use_public_ips