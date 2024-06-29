import boto3
import os
import sys

# Setup boto3 to use LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
endpoint_url = 'http://localhost:4566'

sns = boto3.client('sns', endpoint_url=endpoint_url)

def create_or_get_topic(topic_name):
    try:
        response = sns.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
        print(f"Created topic: {topic_name} with ARN: {topic_arn}")
    except sns.exceptions.TopicLimitExceededException:
        print(f"Topic {topic_name} already exists")
        response = sns.list_topics()
        topics = response['Topics']
        topic_arn = next((t['TopicArn'] for t in topics if t['TopicArn'].endswith(topic_name)), None)
        if topic_arn:
            print(f"Using existing topic: {topic_name} with ARN: {topic_arn}")
        else:
            raise Exception(f"Topic {topic_name} not found even though it should exist!")
    return topic_arn

def publish_message(event_type, message):
    if event_type == 'broadcast':
        topic_arn = create_or_get_topic('broadcast')
    elif event_type == 'communication':
        topic_arn = create_or_get_topic('communication')
    elif event_type == 'entity':
        topic_arn = create_or_get_topic('entity')
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
