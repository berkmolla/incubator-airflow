# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import unittest
import mock
import boto3

try:
    from moto import mock_sns
except ImportError:
    mock_sns = None

from airflow.operators.sns_operator import SNSPublishOperator


@unittest.skipIf(mock_sns is None, 'Skipping unit test as failed to import moto')
class SNSPublishOperatorTest(unittest.TestCase):

    @mock.patch('airflow.hooks.sns_hook.SNSHook.publish')
    @mock_sns
    def test_publish(self, mock_publish):
        test_message = {'msg': 'test'}

        # Initialize SNS client and set up test topic
        conn = boto3.client('sns', 'eu-west-1')
        topic_create_response = conn.create_topic(Name='test')

        # Set up operator and call the publish method
        operator = SNSPublishOperator(task_id='test', message=test_message,
                                      topic_arn=topic_create_response['TopicArn'])
        operator.execute(None)

        mock_publish.assert_called_once_with(message=json.dumps(test_message),
                                             topic_arn=topic_create_response['TopicArn'],
                                             message_structure=None,
                                             phone_number=None,
                                             subject=None,
                                             target_arn=None,
                                             message_attributes={})
