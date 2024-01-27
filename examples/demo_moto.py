"""Run the CLI end to end against moto's in-memory SQS. Nothing reaches AWS.

Creates two queues in the mock (orders-dlq and orders-inbox), loads the sample
messages from dlq_messages.json into orders-dlq, then runs peek, a redrive dry
run and a redrive with --apply, printing the queue depths in between.

    pip install "moto[sqs]"
    python examples/demo_moto.py
"""

import json
import os
import shlex
import sys
from pathlib import Path

os.environ.update(
    AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing", AWS_SESSION_TOKEN="testing",
    AWS_DEFAULT_REGION="us-east-1",
    AWS_SHARED_CREDENTIALS_FILE="/nonexistent/credentials", AWS_CONFIG_FILE="/nonexistent/config",
)

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from lambda_dlq_redrive.cli import main  # noqa: E402

SAMPLE = Path(__file__).with_name("dlq_messages.json")


def depth(sqs, name):
    url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
    count = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"]
    return f"{name}={count}"


def run(sqs, arguments):
    print("$ lambda-dlq-redrive " + shlex.join(arguments))
    sys.stdout.flush()
    code = main(arguments + ["--wait", "0"], sleep=lambda seconds: None)
    print(f"[exit {code}; queue depths: {depth(sqs, 'orders-dlq')}, {depth(sqs, 'orders-inbox')}]\n")


with mock_aws():
    sqs = boto3.client("sqs", region_name="us-east-1")
    dlq = sqs.create_queue(QueueName="orders-dlq")["QueueUrl"]
    sqs.create_queue(QueueName="orders-inbox")
    for message in json.loads(SAMPLE.read_text())["Messages"]:
        sqs.send_message(QueueUrl=dlq, MessageBody=message["Body"],
                         MessageAttributes=message["MessageAttributes"])

    run(sqs, ["peek", "--queue", "orders-dlq", "--group-by", "error"])
    run(sqs, ["redrive", "--queue", "orders-dlq", "--target", "orders-inbox", "--error-contains", "timed out"])
    run(sqs, ["redrive", "--queue", "orders-dlq", "--target", "orders-inbox", "--error-contains", "timed out",
              "--apply", "--yes"])
