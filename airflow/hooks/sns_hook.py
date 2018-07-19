import logging

from airflow import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook


class SNSHook(AwsHook):

    def __init__(self, aws_conn_id='aws_default', region_name=None):
        super(SNSHook, self).__init__(aws_conn_id=aws_conn_id)
        self.region_name = region_name

    def get_conn(self):
        client = self.get_client_type('sns', self.region_name)
        return client

    def publish(self, topic_arn=None,
                target_arn=None,
                phone_number=None,
                message=None,
                subject=None,
                message_structure=None,
                message_attributes={}):

        params = {}
        if subject:
            params['Subject'] = subject
        if message_structure:
            params['MessageStructure'] = message_structure
        if message_attributes:
            params['MessageAttributes'] = message_attributes

        possible_values = [topic_arn, target_arn, phone_number]
        present_values = [x for x in possible_values if x is not None]

        if len(present_values) > 1:
            raise AirflowException('You may only include only one out of TopicArn, TargetArn and PhoneNumber')
        if len(present_values) is 0:
            raise AirflowException('Either TopicArn, TargetArn or PhoneNumber must be specified')

        if topic_arn:
            params['TopicArn'] = topic_arn
        elif target_arn:
            params['TargetArn'] = target_arn
        elif phone_number:
            params['PhoneNumber'] = phone_number

        try:
            response = self.get_conn().publish(
                Message=message,
                **params
            )

            logging.info('Successfully published message to topic')
            return response
        except Exception as err:
            raise AirflowException('Failed to publish message to topic with error: {error}'.format(error=err))
