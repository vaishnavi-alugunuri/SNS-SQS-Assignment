import boto3
import os
import sys
import time

# Setup boto3 to use LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
endpoint_url = 'http://localhost:4566'

sqs = boto3.client('sqs', endpoint_url=endpoint_url)

def consume_messages(queue_name):
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )

        messages = response.get('Messages', [])
        for message in messages:
            print(f"Received message: {message['Body']}")
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <queue_name>")
        sys.exit(1)
    
    queue_name = sys.argv[1]  # 'email', 'sms', or 'entity'
    consume_messages(queue_name)
