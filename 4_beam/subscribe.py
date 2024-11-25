# from google.cloud import pubsub_v1
# import time
# import os

# if __name__ == "__main__":
    
#     # Replace 'my-service-account-path' with your service account path
#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'service-account.json'
    
#     # Replace 'my-subscription' with your subscription id
#     subscription_path = 'projects/danh-nguyen-403304/subscriptions/sink-sub'
    
#     subscriber = pubsub_v1.SubscriberClient()
 
#     def callback(message):
#         print(('Received message: {}'.format(message)))    
#         message.ack()

#     subscriber.subscribe(subscription_path, callback=callback)

#     while True:
        # time.sleep(60)
        
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'service-account.json'
# TODO(developer)
project_id = "danh-nguyen-403304"
subscription_id = 'sink-sub'
# Number of seconds the subscriber should listen for messages
timeout = 0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.