import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def fake_aws_credentials(monkeypatch):
    """Make sure no test can reach a real AWS account."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent/config")


@pytest.fixture
def sqs(fake_aws_credentials):
    with mock_aws():
        yield boto3.client("sqs", region_name="us-east-1")


def lambda_failure(sqs, queue_url, body, error="Task timed out after 3.00 seconds", code="200", **extra):
    """Send a message shaped like a failed asynchronous Lambda invocation."""
    attributes = {
        "RequestID": {"DataType": "String", "StringValue": "5e1f3b0a-6b0c-4f7e-9d59-0d1f4f6e2a11"},
        "ErrorCode": {"DataType": "Number", "StringValue": code},
        "ErrorMessage": {"DataType": "String", "StringValue": error},
    }
    return sqs.send_message(QueueUrl=queue_url, MessageBody=body, MessageAttributes=attributes, **extra)["MessageId"]


def drain(sqs, queue_url):
    """Everything currently receivable from a queue (test helper; leaves messages hidden)."""
    messages = []
    while True:
        batch = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, VisibilityTimeout=300,
                                    MessageAttributeNames=["All"],
                                    MessageSystemAttributeNames=["All"]).get("Messages", [])
        if not batch:
            return messages
        messages.extend(batch)


def visible_count(sqs, queue_url):
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"]), int(attributes["ApproximateNumberOfMessagesNotVisible"])
