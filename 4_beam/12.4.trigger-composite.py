import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam.transforms.window import FixedWindows, TimestampedValue, GlobalWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly, AfterAny

# Replace with your service account path
service_account_path = 'service-account.json'

print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = 'projects/danh-nguyen-403304/subscriptions/source-sub'

# Replace with your output subscription id
output_topic = 'projects/danh-nguyen-403304/topics/sink'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

def encode_byte_string(element):

   element = str(element)
   return element.encode('utf-8')

def decode_byte_string(element):

   return element.decode('utf-8')

def custom_timestamp(elements):
  unix_timestamp = elements[7]
  return TimestampedValue(elements, int(unix_timestamp))

def calculateProfit(elements):
  buy_rate = elements[5]
  sell_price = elements[6]
  products_count = int(elements[4])
  profit = (int(sell_price) - int(buy_rate)) * products_count
  elements.append(str(profit))
  return elements

pubsub_data= (
                p 
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription,timestamp_attribute = 1553578219)  
                # STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219/r/n
                | 'Decode byte to string' >> beam.Map(decode_byte_string)  #Pubsub takes data in form of byte strings 
                | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))          # STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219
                | 'Split Row' >> beam.Map(lambda row : row.split(','))                             # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219]
                | 'Filter By Country' >> beam.Filter(lambda elements : (elements[1] == "Mumbai" or elements[1] == "Bangalore"))
                | 'Create Profit Column' >> beam.Map(calculateProfit)                              # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219,27]
                # | 'Apply custom timestamp' >> beam.Map(custom_timestamp) 
                | 'Form Key Value pair' >> beam.Map(lambda elements : (elements[0], int(elements[6])))  # STR_2 27
                | 'Window' >> beam.WindowInto(FixedWindows(20), trigger=Repeatedly(AfterAny(AfterCount(5), AfterProcessingTime(20))), accumulation_mode=AccumulationMode.DISCARDING)
                | 'Sum values' >> beam.CombinePerKey(sum)
                | 'Encode to byte string' >> beam.Map(encode_byte_string)  #Pubsub takes data in form of byte strings 
                | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)
	             )

result = p.run()
result.wait_until_finish()