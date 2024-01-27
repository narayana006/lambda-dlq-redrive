"""Pure functions over SQS messages (the dicts returned by ReceiveMessage)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

Message = dict[str, Any]
NO_ERROR = "(no error information)"

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_LONG_NUMBER = re.compile(r"\d{5,}")
_ISO_TIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")


def attribute_value(message: Message, name: str) -> Optional[str]:
    """String value of a message attribute, or None (binary attributes have no string value)."""
    attribute = (message.get("MessageAttributes") or {}).get(name)
    if not attribute:
        return None
    return attribute.get("StringValue")


def normalise(text: str) -> str:
    """Replace request IDs, timestamps and long numbers so similar errors group together."""
    text = _ISO_TIME.sub("<time>", text)
    text = _UUID.sub("<uuid>", text)
    text = _LONG_NUMBER.sub("<n>", text)
    return " ".join(text.split())


def error_key(message: Message) -> str:
    """A short description of why the message failed, when the message says so.

    Two sources are understood:

    * the ``ErrorMessage`` / ``ErrorCode`` message attributes Lambda adds when
      it sends a failed asynchronous invocation to a function's dead-letter queue;
    * a Lambda on-failure destination record in the body (``responsePayload``
      with ``errorType`` / ``errorMessage``, or ``requestContext.condition``).

    Messages moved by an SQS redrive policy carry no error information.
    """
    error_message = attribute_value(message, "ErrorMessage")
    if error_message:
        code = attribute_value(message, "ErrorCode")
        key = normalise(error_message)
        return f"{key} [ErrorCode {code}]" if code else key
    try:
        body = json.loads(message.get("Body") or "")
    except ValueError:
        return NO_ERROR
    if not isinstance(body, dict):
        return NO_ERROR
    payload = body.get("responsePayload")
    if isinstance(payload, dict) and (payload.get("errorType") or payload.get("errorMessage")):
        parts = [str(payload[k]) for k in ("errorType", "errorMessage") if payload.get(k)]
        return normalise(": ".join(parts))
    context = body.get("requestContext")
    if isinstance(context, dict) and context.get("condition"):
        return str(context["condition"])
    return NO_ERROR


@dataclass(frozen=True)
class Filter:
    """All given conditions must hold. An empty filter matches every message."""

    body_contains: tuple[str, ...] = ()
    attributes: tuple[tuple[str, str], ...] = ()   # (name, exact string value)
    error_contains: tuple[str, ...] = ()

    def matches(self, message: Message) -> bool:
        body = message.get("Body") or ""
        if any(text not in body for text in self.body_contains):
            return False
        if any(attribute_value(message, name) != value for name, value in self.attributes):
            return False
        if self.error_contains:
            key = error_key(message).lower()
            if any(text.lower() not in key for text in self.error_contains):
                return False
        return True

    def __bool__(self) -> bool:
        return bool(self.body_contains or self.attributes or self.error_contains)


def group_key(message: Message, group_by: str) -> str:
    if group_by == "error":
        return error_key(message)
    if group_by.startswith("attr:"):
        name = group_by[len("attr:"):]
        value = attribute_value(message, name)
        return f"{name}={value}" if value is not None else f"({name} not set)"
    raise ValueError(f"unknown grouping: {group_by}")


@dataclass
class Group:
    key: str
    count: int = 0
    example_ids: list[str] = field(default_factory=list)


def group_messages(messages: Iterable[Message], group_by: str = "error") -> list[Group]:
    """Groups sorted by size, largest first."""
    groups: dict[str, Group] = {}
    for message in messages:
        key = group_key(message, group_by)
        group = groups.setdefault(key, Group(key))
        group.count += 1
        if len(group.example_ids) < 1:
            group.example_ids.append(message.get("MessageId", "?"))
    return sorted(groups.values(), key=lambda g: (-g.count, g.key))


def load_messages(text: str) -> list[Message]:
    """Messages from saved ``aws sqs receive-message`` output, or a JSON list of messages."""
    data = json.loads(text)
    if isinstance(data, dict):
        data = data.get("Messages", [])
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise ValueError('expected {"Messages": [...]} or a JSON list of messages')
    return data
