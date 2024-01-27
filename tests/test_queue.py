"""Moving messages, against moto's SQS. Nothing here talks to AWS."""

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from conftest import drain, lambda_failure, visible_count
from lambda_dlq_redrive.messages import Filter
from lambda_dlq_redrive.queue import (FunctionTarget, QueueTarget, RedriveError, batch_size_for,
                                      copy_attributes, find_source_queue, process, resolve_queue_url)

FAST = {"wait_seconds": 0, "sleep": lambda seconds: None}


@pytest.fixture
def queues(sqs):
    dlq = sqs.create_queue(QueueName="orders-dlq")["QueueUrl"]
    target = sqs.create_queue(QueueName="orders")["QueueUrl"]
    return dlq, target


def statuses(result):
    return [outcome.status for outcome in result.outcomes]


def test_peek_removes_nothing_and_makes_messages_visible_again(sqs, queues):
    dlq, _ = queues
    for index in range(12):
        lambda_failure(sqs, dlq, f'{{"n": {index}}}')
    result = process(sqs, dlq, **FAST)
    assert result.received == 12
    assert len(result.matched_messages) == 12
    assert set(statuses(result)) == {"would-move"}
    assert result.release_failures == 0
    assert visible_count(sqs, dlq) == (12, 0)


def test_dry_run_sends_nothing(sqs, queues):
    dlq, target = queues
    lambda_failure(sqs, dlq, "{}")
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=False, **FAST)
    assert statuses(result) == ["would-move"]
    assert visible_count(sqs, target) == (0, 0)
    assert visible_count(sqs, dlq) == (1, 0)


def test_apply_moves_body_and_all_attribute_types(sqs, queues):
    dlq, target = queues
    attributes = {
        "tenant": {"DataType": "String", "StringValue": "acme"},
        "attempt": {"DataType": "Number", "StringValue": "3"},
        "weight": {"DataType": "Number.float", "StringValue": "1.5"},
        "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01\xff"},
    }
    original = sqs.send_message(QueueUrl=dlq, MessageBody='{"order_id": "ord-1"}',
                                MessageAttributes=attributes)["MessageId"]
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, **FAST)
    assert statuses(result) == ["moved"]
    assert visible_count(sqs, dlq) == (0, 0)
    (moved,) = drain(sqs, target)
    assert moved["Body"] == '{"order_id": "ord-1"}'
    assert moved["MessageAttributes"] == attributes
    assert moved["MessageId"] != original                       # it is a new message
    assert moved["MessageId"] == result.outcomes[0].detail
    assert moved["Attributes"]["ApproximateReceiveCount"] == "1"  # receive count starts again
    assert "MessageGroupId" not in moved["Attributes"]


def test_many_messages_are_all_moved(sqs, queues):
    dlq, target = queues
    for index in range(35):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"m{index}")
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, max_messages=0, **FAST)
    assert result.count("moved") == 35
    assert sorted(m["Body"] for m in drain(sqs, target)) == sorted(f"m{i}" for i in range(35))
    assert visible_count(sqs, dlq) == (0, 0)


def test_filter_moves_only_matching_and_releases_the_rest(sqs, queues):
    dlq, target = queues
    for index in range(4):
        lambda_failure(sqs, dlq, f'{{"n": {index}}}')
    for index in range(3):
        lambda_failure(sqs, dlq, f'{{"k": {index}}}', error="KeyError: 'customer_id'")
    result = process(sqs, dlq, Filter(error_contains=("timed out",)), QueueTarget(sqs, target), apply=True, **FAST)
    assert result.count("moved") == 4 and result.count("skipped") == 3
    assert sorted(m["Body"] for m in drain(sqs, target)) == [f'{{"n": {i}}}' for i in range(4)]
    assert visible_count(sqs, dlq) == (3, 0)


def test_max_messages_limits_what_is_moved(sqs, queues):
    dlq, target = queues
    for index in range(8):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"m{index}")
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, max_messages=3, **FAST)
    assert result.count("moved") == 3
    assert visible_count(sqs, target) == (3, 0)
    assert visible_count(sqs, dlq) == (5, 0)


def test_message_is_still_in_the_dlq_while_it_is_being_sent(sqs, queues):
    dlq, target = queues
    for index in range(3):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"m{index}")
    inner = QueueTarget(sqs, target)
    seen_during_send = []

    class Watching:
        description = "watching"

        def send(self, message):
            visible, hidden = visible_count(sqs, dlq)
            seen_during_send.append(visible + hidden)
            return inner.send(message)

    process(sqs, dlq, target=Watching(), apply=True, **FAST)
    # 3 present for the first send, 2 for the second (one already moved), 1 for the third
    assert seen_during_send == [3, 2, 1]


def test_failed_send_leaves_the_message_in_the_dlq(sqs, queues):
    dlq, _ = queues
    for index in range(3):
        lambda_failure(sqs, dlq, f"m{index}")
    missing = QueueTarget(sqs, "https://sqs.us-east-1.amazonaws.com/123456789012/does-not-exist")
    result = process(sqs, dlq, target=missing, apply=True, **FAST)
    assert statuses(result) == ["failed"] * 3
    assert "send failed" in result.outcomes[0].detail
    assert visible_count(sqs, dlq) == (3, 0)           # nothing deleted, all visible again
    assert len(drain(sqs, dlq)) == 3


def test_partial_failure_only_deletes_what_was_sent(sqs, queues):
    dlq, target = queues
    for index in range(5):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"m{index}")
    inner = QueueTarget(sqs, target)

    class Flaky:
        description = "flaky"

        def send(self, message):
            if message["Body"] in ("m1", "m3"):
                raise RedriveError("simulated failure")
            return inner.send(message)

    result = process(sqs, dlq, target=Flaky(), apply=True, **FAST)
    assert result.count("moved") == 3 and result.count("failed") == 2
    assert sorted(m["Body"] for m in drain(sqs, target)) == ["m0", "m2", "m4"]
    assert sorted(m["Body"] for m in drain(sqs, dlq)) == ["m1", "m3"]


def test_failed_delete_is_reported_as_a_duplicate_not_as_moved(sqs, queues):
    dlq, target = queues
    sqs.send_message(QueueUrl=dlq, MessageBody="m0")

    class NoDelete:
        def __getattr__(self, name):
            return getattr(sqs, name)

        def delete_message(self, **kwargs):
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no sqs:DeleteMessage"}}, "DeleteMessage")

    result = process(NoDelete(), dlq, target=QueueTarget(sqs, target), apply=True, **FAST)
    assert statuses(result) == ["sent-not-deleted"]
    assert visible_count(sqs, target) == (1, 0)
    visible, hidden = visible_count(sqs, dlq)
    assert visible + hidden == 1


def test_rate_limit_sleeps_between_sends(sqs, queues):
    dlq, target = queues
    for index in range(4):
        sqs.send_message(QueueUrl=dlq, MessageBody=f"m{index}")
    sleeps = []
    process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, rate=4, wait_seconds=0, sleep=sleeps.append)
    assert sleeps == [0.25, 0.25, 0.25]


def test_batch_size_keeps_a_batch_inside_the_visibility_timeout():
    assert batch_size_for(5, 60) == 10
    assert batch_size_for(0.1, 60) == 3      # 3 messages take 30 s at one per 10 s
    assert batch_size_for(0.01, 60) == 1


def test_drop_attributes(sqs, queues):
    dlq, target = queues
    lambda_failure(sqs, dlq, "{}")
    drop = ("RequestID", "ErrorCode", "ErrorMessage")
    process(sqs, dlq, target=QueueTarget(sqs, target, drop_attributes=drop), apply=True, **FAST)
    (moved,) = drain(sqs, target)
    assert "MessageAttributes" not in moved
    assert copy_attributes({"MessageAttributes": {"a": {"DataType": "String", "StringValue": "1"}}}, ("a",)) == {}


# ---- FIFO ---------------------------------------------------------------------------------

@pytest.fixture
def fifo_queues(sqs):
    dlq = sqs.create_queue(QueueName="orders-dlq.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    target = sqs.create_queue(QueueName="orders.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    return dlq, target


def test_fifo_keeps_group_and_order_and_uses_a_new_deduplication_id(sqs, fifo_queues):
    dlq, target = fifo_queues
    for group in ("g1", "g2"):
        for index in range(3):
            sqs.send_message(QueueUrl=dlq, MessageBody=f"{group}-m{index}", MessageGroupId=group,
                             MessageDeduplicationId=f"{group}-original-{index}")
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, **FAST)
    assert result.count("moved") == 6
    assert visible_count(sqs, dlq) == (0, 0)
    moved = []
    while True:  # receive and delete so the next message of each group becomes available
        batch = sqs.receive_message(QueueUrl=target, MaxNumberOfMessages=10,
                                    MessageSystemAttributeNames=["All"]).get("Messages", [])
        if not batch:
            break
        for m in batch:
            moved.append(m)
            sqs.delete_message(QueueUrl=target, ReceiptHandle=m["ReceiptHandle"])
    for group in ("g1", "g2"):
        in_group = [m for m in moved if m["Attributes"]["MessageGroupId"] == group]
        assert [m["Body"] for m in in_group] == [f"{group}-m{i}" for i in range(3)]
    dedup_ids = [m["Attributes"]["MessageDeduplicationId"] for m in moved]
    assert len(set(dedup_ids)) == 6
    assert not any("original" in d for d in dedup_ids)


def test_redrive_is_not_swallowed_by_fifo_deduplication(sqs, fifo_queues):
    dlq, target = fifo_queues
    # The target received a message with deduplication ID "d1" a moment ago.
    sqs.send_message(QueueUrl=target, MessageBody="m", MessageGroupId="g", MessageDeduplicationId="d1")
    # Control: a second send with the same ID inside the deduplication interval is accepted
    # by SQS but delivers nothing.
    sqs.send_message(QueueUrl=target, MessageBody="m", MessageGroupId="g", MessageDeduplicationId="d1")
    assert visible_count(sqs, target) == (1, 0)
    # A copy in the dead-letter queue carries the same deduplication ID.
    sqs.send_message(QueueUrl=dlq, MessageBody="m", MessageGroupId="g", MessageDeduplicationId="d1")
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, **FAST)
    assert statuses(result) == ["moved"]
    assert visible_count(sqs, target) == (2, 0)   # delivered, because the redrive used a new ID
    assert visible_count(sqs, dlq) == (0, 0)


def test_fifo_failure_blocks_the_rest_of_that_group_only(sqs, fifo_queues):
    dlq, target = fifo_queues
    for group in ("g1", "g2"):
        for index in range(3):
            sqs.send_message(QueueUrl=dlq, MessageBody=f"{group}-m{index}", MessageGroupId=group,
                             MessageDeduplicationId=f"{group}-{index}")
    inner = QueueTarget(sqs, target)

    class Flaky:
        description = "flaky"

        def send(self, message):
            if message["Body"] == "g1-m1":
                raise RedriveError("simulated failure")
            return inner.send(message)

    result = process(sqs, dlq, target=Flaky(), apply=True, **FAST)
    by_id = {o.message_id: o for o in result.outcomes}
    assert result.count("moved") == 4 and result.count("failed") == 2
    details = sorted(o.detail for o in by_id.values() if o.status == "failed")
    assert details == ["not sent: an earlier message of group g1 failed", "send failed: simulated failure"]
    left = drain(sqs, dlq)
    assert [m["Body"] for m in left] == ["g1-m1", "g1-m2"]   # still there, still in order


def test_standard_dlq_to_fifo_target_needs_a_group_id(sqs):
    dlq = sqs.create_queue(QueueName="plain-dlq")["QueueUrl"]
    target = sqs.create_queue(QueueName="orders.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    sqs.send_message(QueueUrl=dlq, MessageBody="m0")
    result = process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, **FAST)
    assert statuses(result) == ["failed"]
    assert "--message-group-id" in result.outcomes[0].detail
    assert visible_count(sqs, dlq) == (1, 0)
    result = process(sqs, dlq, target=QueueTarget(sqs, target, message_group_id="replay"), apply=True, **FAST)
    assert statuses(result) == ["moved"]
    (moved,) = drain(sqs, target)
    assert moved["Attributes"]["MessageGroupId"] == "replay"


def test_fifo_dlq_to_standard_target_sends_no_group_id(sqs, fifo_queues):
    dlq, _ = fifo_queues
    target = sqs.create_queue(QueueName="plain")["QueueUrl"]
    sqs.send_message(QueueUrl=dlq, MessageBody="m0", MessageGroupId="g", MessageDeduplicationId="d")
    assert statuses(process(sqs, dlq, target=QueueTarget(sqs, target), apply=True, **FAST)) == ["moved"]
    (moved,) = drain(sqs, target)
    assert "MessageGroupId" not in moved["Attributes"]
    assert "MessageDeduplicationId" not in moved["Attributes"]


# ---- targets and lookups -------------------------------------------------------------------

def test_resolve_queue_url(sqs, queues):
    dlq, _ = queues
    assert resolve_queue_url(sqs, "orders-dlq") == dlq
    assert resolve_queue_url(sqs, dlq) == dlq
    with pytest.raises(ClientError):
        resolve_queue_url(sqs, "nope")


def test_find_source_queue(sqs):
    dlq = sqs.create_queue(QueueName="jobs-dlq")["QueueUrl"]
    with pytest.raises(RedriveError, match="no SQS queue uses"):
        find_source_queue(sqs, dlq)
    arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    policy = '{"deadLetterTargetArn": "%s", "maxReceiveCount": "3"}' % arn
    source = sqs.create_queue(QueueName="jobs", Attributes={"RedrivePolicy": policy})["QueueUrl"]
    assert find_source_queue(sqs, dlq) == source
    sqs.create_queue(QueueName="jobs-2", Attributes={"RedrivePolicy": policy})
    with pytest.raises(RedriveError, match="more than one queue"):
        find_source_queue(sqs, dlq)


def test_function_target_invokes_asynchronously_then_deletes(sqs, queues):
    dlq, _ = queues
    lambda_failure(sqs, dlq, '{"order_id": "ord-1"}')
    client = boto3.client("lambda", region_name="us-east-1")
    with Stubber(client) as stubber:
        stubber.add_response("invoke", {"StatusCode": 202}, {
            "FunctionName": "orders-handler", "InvocationType": "Event", "Payload": b'{"order_id": "ord-1"}'})
        result = process(sqs, dlq, target=FunctionTarget(client, "orders-handler"), apply=True, **FAST)
        stubber.assert_no_pending_responses()
    assert statuses(result) == ["moved"]
    assert visible_count(sqs, dlq) == (0, 0)


def test_function_target_failure_keeps_the_message(sqs, queues):
    dlq, _ = queues
    lambda_failure(sqs, dlq, "not json")
    lambda_failure(sqs, dlq, "{}")
    client = boto3.client("lambda", region_name="us-east-1")
    with Stubber(client) as stubber:
        stubber.add_client_error("invoke", "InvalidRequestContentException", "Could not parse request body into json")
        stubber.add_response("invoke", {"StatusCode": 200})   # anything but 202 is a failure too
        result = process(sqs, dlq, target=FunctionTarget(client, "orders-handler"), apply=True, **FAST)
    assert statuses(result) == ["failed", "failed"]
    assert visible_count(sqs, dlq) == (2, 0)
