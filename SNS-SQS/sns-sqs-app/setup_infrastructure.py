import boto3
import os

# Setup boto3 to use LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
endpoint_url = 'http://localhost:4566'

sns = boto3.client('sns', endpoint_url=endpoint_url)
sqs = boto3.client('sqs', endpoint_url=endpoint_url)

# Create SNS topics
broadcast_topic_arn = sns.create_topic(Name='broadcast')['TopicArn']
communication_topic_arn = sns.create_topic(Name='communication')['TopicArn']
entity_topic_arn = sns.create_topic(Name='entity')['TopicArn']

# Create SQS queues
email_queue_url = sqs.create_queue(QueueName='email')['QueueUrl']
sms_queue_url = sqs.create_queue(QueueName='sms')['QueueUrl']
entity_queue_url = sqs.create_queue(QueueName='entity')['QueueUrl']

# Get the ARN of each queue
email_queue_arn = sqs.get_queue_attributes(QueueUrl=email_queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
sms_queue_arn = sqs.get_queue_attributes(QueueUrl=sms_queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
entity_queue_arn = sqs.get_queue_attributes(QueueUrl=entity_queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']

# Subscribe queues to topics using their ARNs
sns.subscribe(TopicArn=broadcast_topic_arn, Protocol='sqs', Endpoint=email_queue_arn)
sns.subscribe(TopicArn=broadcast_topic_arn, Protocol='sqs', Endpoint=sms_queue_arn)
sns.subscribe(TopicArn=broadcast_topic_arn, Protocol='sqs', Endpoint=entity_queue_arn)

sns.subscribe(TopicArn=communication_topic_arn, Protocol='sqs', Endpoint=email_queue_arn)
sns.subscribe(TopicArn=communication_topic_arn, Protocol='sqs', Endpoint=sms_queue_arn)

sns.subscribe(TopicArn=entity_topic_arn, Protocol='sqs', Endpoint=entity_queue_arn)

print("SNS and SQS setup completed.")
