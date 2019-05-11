"""
Copyright (C) 2019 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
"""

from locust import Locust, TaskSet, task, events
from google.cloud import pubsub
from faker import Faker
import random
import inspect
import time
import logging

TOPIC_NAME = ''  # Enter PUB/SUB TOPIC
PROJECT_ID = ''  # Enter PROJECT ID
MUTATION_PERCENTAGE = 0.1  # between 0 and 1 (eg: 0.1 = 10%)
EVENT_TYPE = ''  # Enter event-type (eg: game_crash)

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

fake = Faker()
publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


def stopwatch(func):
    def wrapper(*args, **kwargs):
        # get task's function name
        previous_frame = inspect.currentframe().f_back
        _, _, task_name, _, _ = inspect.getframeinfo(previous_frame)

        start = time.time()
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            total = int((time.time() - start) * 1000)
            events.request_failure.fire(request_type="PubSub", name=task_name, response_time=total, exception=e)
        else:
            total = int((time.time() - start) * 1000)
            events.request_success.fire(request_type="PubSub", name=task_name, response_time=total, response_length=0)
        return result

    return wrapper


class PubSubClient:
    def __init__(self):
        self.fake = fake
        self.publisher = publisher
        self.topic_path = topic_path

    @stopwatch  # this is the method we want to measure
    def start_publishing(self):
        schema_to_publish = self.get_schema()
        logging.info(schema_to_publish)
        self.publish_schema(schema_to_publish)

    def get_schema(self):
        data = '"TitleId":"' + fake.color_name() + '", ' \
            '"EventName":"' + EVENT_TYPE + '", ' \
            '"EventId":"' + fake.isbn10(separator="-") + '", ' \
            '"Timestamp":"' + str(fake.date_time(tzinfo=None, end_datetime=None)) + '", ' \
            '"ServerTimestamp":"' + str(fake.date_time(tzinfo=None, end_datetime=None)) + '", ' \
            '"CrashGuid":"' + fake.ean(length=13) + '", ' \
            '"GameChangelist":"' + str(113030) + '", ' \
            '"ErrorMessage":"' + fake.bs() + '"'

        mutation = [', "State":"' + fake.state() + '"', ', "ZipCode":"' + fake.zipcode() + '"']

        if random.random() < MUTATION_PERCENTAGE:
            data = '{' + data + mutation[random.randint(0, len(mutation) - 1)] + '}'
        else:
            data = '{' + data + '}'

        data = data.encode('utf-8')
        return data

    def publish_schema(self, data):
        futures = []
        message_future = self.publisher.publish(self.topic_path, data=data, event_type=EVENT_TYPE)
        futures.append(message_future)


class ProtocolLocust(Locust):
    def __init__(self):
        super(ProtocolLocust, self).__init__()
        self.client = PubSubClient()


class ProtocolTasks(TaskSet):
    @task
    def publish_messages(self):
        self.client.start_publishing()


class ProtocolUser(ProtocolLocust):
    task_set = ProtocolTasks
    min_wait = 2000
    max_wait = 7000
