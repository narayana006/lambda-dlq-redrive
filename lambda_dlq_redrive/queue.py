"""Receiving from a dead-letter queue and moving messages to a target.

The order of operations for a move is fixed: send to the target, and only
after the send call returned successfully, delete from the dead-letter queue.
If the send fails the message is not deleted. If the delete fails the message
exists in both places and is reported as such; it is never the other way round.

Every function takes its boto3 clients as arguments.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol, Sequence

from botocore.exceptions import BotoCoreError, ClientError

from .messages import Filter, Message, error_key

RECEIVE_BATCH = 10


class RedriveError(Exception):
    pass


def resolve_queue_url(sqs: Any, name_or_url: str) -> str:
    if name_or_url.startswith(("https://", "http://")):
        return name_or_url
    return sqs.get_queue_url(QueueName=name_or_url)["QueueUrl"]


def is_fifo(queue_url: str) -> bool:
    return queue_url.endswith(".fifo")


def find_source_queue(sqs: Any, dlq_url: str) -> str:
    """The one SQS queue whose redrive policy points at this dead-letter queue."""
    sources = sqs.list_dead_letter_source_queues(QueueUrl=dlq_url).get("queueUrls", [])
    if len(sources) == 1:
        return sources[0]
    if not sources:
        raise RedriveError("no SQS queue uses this queue as its dead-letter queue; "
                           "pass --target or --target-function")
    raise RedriveError("more than one queue uses this dead-letter queue; pass --target with one of: "
                       + ", ".join(sources))


def copy_attributes(message: Message, drop: Sequence[str] = ()) -> dict[str, dict]:
    """Message attributes in the form SendMessage accepts, minus the dropped names."""
    result = {}
    for name, attribute in (message.get("MessageAttributes") or {}).items():
        if name in drop:
            continue
        copied = {"DataType": attribute["DataType"]}
        if attribute.get("BinaryValue") is not None:
            copied["BinaryValue"] = attribute["BinaryValue"]
        else:
            copied["StringValue"] = attribute.get("StringValue", "")
        result[name] = copied
    return result


class Target(Protocol):
    description: str

    def send(self, message: Message) -> str:
        """Deliver the message and return an identifier of the delivery. Raises on failure."""


class QueueTarget:
    def __init__(self, sqs: Any, queue_url: str, message_group_id: Optional[str] = None,
                 drop_attributes: Sequence[str] = (),
                 new_deduplication_id: Callable[[], str] = lambda: str(uuid.uuid4())):
        self.sqs = sqs
        self.queue_url = queue_url
        self.message_group_id = message_group_id
        self.drop_attributes = tuple(drop_attributes)
        self.new_deduplication_id = new_deduplication_id
        self.description = f"queue {queue_url} ({'FIFO' if is_fifo(queue_url) else 'standard'})"

    def send(self, message: Message) -> str:
        request: dict[str, Any] = {"QueueUrl": self.queue_url, "MessageBody": message["Body"]}
        attributes = copy_attributes(message, self.drop_attributes)
        if attributes:
            request["MessageAttributes"] = attributes
        if is_fifo(self.queue_url):
            group = self.message_group_id or (message.get("Attributes") or {}).get("MessageGroupId")
            if not group:
                raise RedriveError("the target is a FIFO queue and this message has no MessageGroupId; "
                                   "pass --message-group-id")
            request["MessageGroupId"] = group
            # Always a new ID: reusing the original inside the 5-minute deduplication
            # interval would make SQS accept the send and silently drop the message.
            request["MessageDeduplicationId"] = self.new_deduplication_id()
        return self.sqs.send_message(**request)["MessageId"]


class FunctionTarget:
    """Invoke a Lambda function asynchronously with the message body as the event."""

    def __init__(self, lambda_client: Any, function_name: str):
        self.lambda_client = lambda_client
        self.function_name = function_name
        self.description = f"function {function_name} (asynchronous invocation)"

    def send(self, message: Message) -> str:
        response = self.lambda_client.invoke(
            FunctionName=self.function_name,
            InvocationType="Event",
            Payload=message["Body"].encode("utf-8"),
        )
        status = response.get("StatusCode")
        if status != 202:
            raise RedriveError(f"Lambda returned status {status} instead of 202")
        return response.get("ResponseMetadata", {}).get("RequestId", "accepted")


@dataclass
class Outcome:
    message_id: str
    status: str          # moved | would-move | failed | sent-not-deleted | skipped
    detail: str = ""


@dataclass
class Result:
    received: int = 0
    outcomes: list[Outcome] = field(default_factory=list)
    release_failures: int = 0
    matched_messages: list[Message] = field(default_factory=list)

    def count(self, status: str) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == status)


def receive(sqs: Any, queue_url: str, visibility_timeout: int, wait_seconds: int,
            batch_size: int = RECEIVE_BATCH) -> list[Message]:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=batch_size,
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=wait_seconds,
        MessageAttributeNames=["All"],
        MessageSystemAttributeNames=["All"],
    )
    return response.get("Messages", [])


def release(sqs: Any, queue_url: str, messages: Sequence[Message]) -> int:
    """Make held messages visible again. Returns how many could not be released."""
    failures = 0
    for start in range(0, len(messages), 10):
        chunk = messages[start:start + 10]
        entries = [{"Id": str(index), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
                   for index, m in enumerate(chunk)]
        try:
            response = sqs.change_message_visibility_batch(QueueUrl=queue_url, Entries=entries)
            failures += len(response.get("Failed", []))
        except (ClientError, BotoCoreError):
            failures += len(chunk)
    return failures


def batch_size_for(rate: float, visibility_timeout: int) -> int:
    """How many messages to receive at once so a batch is sent well within the visibility timeout."""
    if rate <= 0:
        return RECEIVE_BATCH
    return max(1, min(RECEIVE_BATCH, int(visibility_timeout * rate / 2)))


def process(
    sqs: Any,
    dlq_url: str,
    message_filter: Filter = Filter(),
    target: Optional[Target] = None,
    apply: bool = False,
    max_messages: int = 100,
    rate: float = 5.0,
    visibility_timeout: int = 60,
    wait_seconds: int = 1,
    empty_polls: int = 2,
    sleep: Callable[[float], None] = time.sleep,
    on_outcome: Optional[Callable[[Outcome, Message], None]] = None,
) -> Result:
    """Receive from the dead-letter queue and, with ``apply``, move matching messages.

    Without ``apply`` nothing is sent or deleted: matching messages are only
    collected (this is both ``peek`` and the redrive dry run). Every message
    that was received but not moved is made visible again before returning.

    ``max_messages`` limits matching messages; 0 means no limit. Receiving stops
    after ``empty_polls`` consecutive receives that return nothing new.
    """
    if apply and target is None:
        raise ValueError("apply needs a target")
    result = Result()
    latest: dict[str, Message] = {}   # MessageId -> most recent copy (freshest receipt handle)
    held: list[str] = []              # MessageIds to make visible again at the end
    blocked_groups: set[str] = set()
    matched = 0
    sends = 0
    empty = 0
    size = batch_size_for(rate, visibility_timeout) if apply else RECEIVE_BATCH

    def record(outcome: Outcome, message: Message) -> None:
        result.outcomes.append(outcome)
        if on_outcome:
            on_outcome(outcome, message)

    try:
        while empty < empty_polls and (max_messages == 0 or matched < max_messages):
            batch = receive(sqs, dlq_url, visibility_timeout, wait_seconds, size)
            fresh = [m for m in batch if m["MessageId"] not in latest]
            for message in batch:
                latest[message["MessageId"]] = message
            if not fresh:
                empty += 1
                continue
            empty = 0
            result.received += len(fresh)
            for message in fresh:
                message_id = message["MessageId"]
                if max_messages and matched >= max_messages:
                    held.append(message_id)   # over the limit: untouched, released below
                    continue
                if not message_filter.matches(message):
                    held.append(message_id)
                    record(Outcome(message_id, "skipped", "does not match the filters"), message)
                    continue
                matched += 1
                result.matched_messages.append(message)
                if not apply:
                    held.append(message_id)
                    record(Outcome(message_id, "would-move", error_key(message)), message)
                    continue
                group = (message.get("Attributes") or {}).get("MessageGroupId")
                if group is not None and group in blocked_groups:
                    held.append(message_id)
                    record(Outcome(message_id, "failed",
                                   f"not sent: an earlier message of group {group} failed"), message)
                    continue
                if sends and rate > 0:
                    sleep(1.0 / rate)
                sends += 1
                try:
                    delivery = target.send(message)
                except (ClientError, BotoCoreError, RedriveError) as error:
                    held.append(message_id)
                    if group is not None:
                        blocked_groups.add(group)
                    record(Outcome(message_id, "failed", f"send failed: {error}"), message)
                    continue
                # The send succeeded; only now is the message removed from the dead-letter queue.
                try:
                    sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=message["ReceiptHandle"])
                except (ClientError, BotoCoreError) as error:
                    record(Outcome(message_id, "sent-not-deleted",
                                   f"sent as {delivery} but the delete failed: {error}"), message)
                    continue
                record(Outcome(message_id, "moved", delivery), message)
    finally:
        if held:
            result.release_failures = release(sqs, dlq_url, [latest[i] for i in held])
    return result
