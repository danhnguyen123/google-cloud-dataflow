import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
from datetime import datetime

# Replace with your service account path
service_account_path = ''

print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = ''

# Replace with your output subscription id
output_topic = ''

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

def custom_timestamp(elements):
  unix_timestamp = elements[16].rstrip().lstrip()
  return beam.window.TimestampedValue(elements, int(unix_timestamp))

def encode_byte_string(element):
  
  element = str(element)
  print(element)
  return element.encode('utf-8')

def calculate_battle_points(element_list):     # [GM_1,PL_1,Allison,TM_01,Blasters,BN60,6,MP_100,PL_16,Odette,TM_03,Masters,Bomb,1,MP_113,20,1553578221/r/n]
    # logic for point calculation with a weapon

    total_points = 0
    game_id = element_list[0]
    player_id = element_list[1]                                        #player_id = PL_1
    weapon = element_list[5]                                           # weapon = BN60

    my_weapon_ranking = element_list[6].rstrip().lstrip()             # my_weapon_ranking = 6
    my_weapon_ranking = int(my_weapon_ranking)
    opp_weapon_ranking = element_list[13].rstrip().lstrip()           # opp_weapon_ranking = 1
    opp_weapon_ranking = int(opp_weapon_ranking)
    
    my_map_location = element_list[7].rstrip().lstrip()               # my_map_location = MP_100
    opp_map_location = element_list[14].rstrip().lstrip()             # opp_map_location = MP_113
    
    battle_time = element_list[15]
    battle_time = int(battle_time.rstrip().lstrip())                  # battle_time = 20

    if battle_time >= 10 and battle_time <= 20:
        total_points += 4                                             # total_points = 4
    elif battle_time >=21 and battle_time <= 30:
        total_points += 3
    elif battle_time >=31 and battle_time <=40:
        total_points += 2
    elif battle_time > 40:
        total_points += 1

    diff = my_weapon_ranking - opp_weapon_ranking

    if diff >= 6:
       total_points += 3 

    elif diff >= 3:
        total_points += 2                                           # total_points = 6
    else: 
        total_points += 1     

    if my_map_location != opp_map_location:                         # total_points = 9
        total_points += 3         

    return game_id + ':' + player_id + ':' + weapon, total_points                    # GM_1:PL_1:BN60, 9

class PointFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0)

  def add_input(self, sum_count, input):                         # Intial sum_count = 0.0,0
    (sum, count) = sum_count                                     # input = 9   
    return sum + input, count + 1                                # returned pair = 9 , 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)                            # zip - [(9,1) , (20,4)]  --> [(9,20),(1,4)]
    return sum(sums), sum(counts)                                # return  (29, 5)

  def extract_output(self, sum_count):
    (sum, count) = sum_count                                   
    return sum / count if count else float('NaN')                # return  = 5.8                   

def format_result(key_value_pair):

    name, points = key_value_pair
    name_list = name.split(':')
    game_id = name_list[0]
    player_id = name_list[1]
    weapon = ' '.join(name_list[2:])
    return  game_id + ',' + player_id + ', ' + weapon + ', ' + str(points) + ' average battle points '



pubsub_data = (
                p 
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
				# GM_1,PL_1,Allison,TM_01,Blasters,BN60,6,MP_100,PL_16,Odette,TM_03,Masters,Bomb,1,MP_113,20,1553578221/r/n

                | 'Parse data' >> beam.Map(lambda element: element.split(','))
               # | 'Apply custom timestamp' >> beam.Map(custom_timestamp)
                | 'Calculate battle points' >> beam.Map(calculate_battle_points)        #  Key = GM_1:PL_1:BN60 value = 9 
                | 'Window for player' >> beam.WindowInto(window.Sessions(30))
                | 'Group by key' >> beam.CombinePerKey(PointFn())                    # output --> GM_1:PL_1:BN60, average points
                | 'Format results' >> beam.Map(format_result)    
                | 'Encode data to byte string' >> beam.Map(encode_byte_string)
                | 'Write player score to pub sub' >> beam.io.WriteToPubSub(output_topic)
              )


result = p.run()
result.wait_until_finish()