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

from airflow.hooks.sns_hook import SNSHook
from airflow.models import BaseOperator

class SNSPublishOperator(BaseOperator):

    def __init__(self,
                 conn_id='aws_default',
                 region_name=None,
                 topic_arn=None,
                 target_arn=None,
                 phone_number=None,
                 message=None,
                 subject=None,
                 message_structure=None,
                 message_attributes={},
                 *args,
                 **kwargs):

        super(SNSPublishOperator, self).__init__(*args, **kwargs)

        try:
            self.message = json.dumps(message)
        except Exception:
            raise ValueError("invalid JSON in message")

        self.conn_id = conn_id
        self.topic_arn = topic_arn
        self.target_arn = target_arn
        self.phone_number = phone_number
        self.subject = subject
        self.message_structure = message_structure
        self.message_attributes = message_attributes
        self.region_name = region_name

    def execute(self, context):
        hook = SNSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region_name
        )

        hook.publish(
            topic_arn=self.topic_arn,
            target_arn=self.target_arn,
            phone_number=self.phone_number,
            message=self.message,
            subject=self.subject,
            message_structure=self.message_structure,
            message_attributes=self.message_attributes
        )
