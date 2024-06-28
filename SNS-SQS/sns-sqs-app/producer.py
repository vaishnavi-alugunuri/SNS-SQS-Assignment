import boto3
import os
import sys

# Setup boto3 to use LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
endpoint_url = 'http://localhost:4566'

sns = boto3.client('sns', endpoint_url=endpoint_url)

def publish_message(event_type, message):
    if event_type == 'broadcast':
        topic_arn = 'arn:aws:sns:us-east-1:000000000000:broadcast'
    elif event_type == 'communication':
        topic_arn = 'arn:aws:sns:us-east-1:000000000000:communication'
    elif event_type == 'entity':
        topic_arn = 'arn:aws:sns:us-east-1:000000000000:entity'
    else:
        print("Invalid event type")
        return
    
    sns.publish(TopicArn=topic_arn, Message=message)
    print(f"Message published to {event_type} topic")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python producer.py <event_type> <message>")
        sys.exit(1)
    
    event_type = sys.argv[1]  # 'broadcast', 'communication', or 'entity'
    message = sys.argv[2]
    publish_message(event_type, message)
