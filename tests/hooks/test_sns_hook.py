import json
import unittest

from airflow import configuration

try:
    from airflow.hooks.sns_hook import SNSHook
except ImportError:
    SNSHook = None

try:
    import boto3
    from moto import mock_sns, mock_sqs
except ImportError:
    mock_sns = None
    mock_sqs = None


@unittest.skipIf(SNSHook is None, "Skipping test because SNSHook is not available")
@unittest.skipIf(mock_sns is None, "Skipping test because moto.mock_sns is not available")
class TestSNSHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

    @mock_sns
    @mock_sqs
    def test_publish(self):
        test_message = json.dumps({'msg': 'test'})

        # Initialize SNS client and set up test topic
        conn = boto3.client('sns', 'eu-west-1')
        topic_create_response = conn.create_topic(Name='test')

        # Initialize the SQS client, create a queue
        # and subscribe to the SNS topic created above
        sqs_conn = boto3.client('sqs', 'eu-west-1')
        create_queue_response = sqs_conn.create_queue(QueueName='test')
        queue_attrs = sqs_conn.get_queue_attributes(
            QueueUrl=create_queue_response['QueueUrl'],
            AttributeNames=['QueueArn'])
        conn.subscribe(TopicArn=topic_create_response['TopicArn'],
                       Protocol='sqs',
                       Endpoint=queue_attrs['Attributes']['QueueArn'])

        # Publish the test message to our SNS topic with the SNSHook
        hook = SNSHook(aws_conn_id=None, region_name='eu-west-1')
        publish_response = hook.publish(topic_arn=topic_create_response['TopicArn'],
                                        message=test_message)

        # Pull message that was sent from SNS to the SQS queue
        messages = sqs_conn.receive_message(QueueUrl=create_queue_response['QueueUrl'],
                                            MaxNumberOfMessages=1)
        first_message = messages['Messages'][0]
        message_body = json.loads(first_message['Body'])

        # Assert
        expected_message = test_message
        expected_topic_arn = topic_create_response['TopicArn']
        expected_message_id = publish_response['MessageId']
        self.assertEqual(expected_message, message_body['Message'])
        self.assertEqual(expected_topic_arn, message_body['TopicArn'])
        self.assertEqual(expected_message_id, message_body['MessageId'])
