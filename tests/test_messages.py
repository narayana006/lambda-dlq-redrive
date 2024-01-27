import json
from pathlib import Path

import pytest

from lambda_dlq_redrive.messages import (NO_ERROR, Filter, error_key, group_messages, load_messages,
                                         normalise)

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "dlq_messages.json"


def message(body="{}", **attributes):
    return {"MessageId": "id-1", "Body": body,
            "MessageAttributes": {k: {"DataType": "String", "StringValue": v} for k, v in attributes.items()}}


def test_error_key_from_lambda_dlq_attributes():
    m = message(ErrorMessage="Task timed out after 3.00 seconds", ErrorCode="200")
    assert error_key(m) == "Task timed out after 3.00 seconds [ErrorCode 200]"
    assert error_key(message(ErrorMessage="boom")) == "boom"


def test_request_ids_and_timestamps_do_not_split_groups():
    a = "2026-01-05T10:10:03.004Z 5e1f3b0a-6b0c-4f7e-9d59-0d1f4f6e2a11 Task timed out after 3.00 seconds"
    b = "2026-01-05T11:42:17.950Z 0a2b3c4d-1111-4222-8333-444455556666 Task timed out after 3.00 seconds"
    assert normalise(a) == normalise(b) == "<time> <uuid> Task timed out after 3.00 seconds"
    assert normalise("order 1234567 failed") == "order <n> failed"
    assert normalise("status 500") == "status 500"


def test_error_key_from_destination_record_body():
    record = {"version": "1.0", "requestContext": {"condition": "RetriesExhausted"},
              "requestPayload": {"order_id": 1},
              "responsePayload": {"errorType": "KeyError", "errorMessage": "'customer_id'"}}
    assert error_key(message(json.dumps(record))) == "KeyError: 'customer_id'"
    del record["responsePayload"]
    assert error_key(message(json.dumps(record))) == "RetriesExhausted"


def test_error_key_without_information():
    assert error_key(message('{"order_id": 1}')) == NO_ERROR
    assert error_key(message("not json")) == NO_ERROR
    assert error_key(message("[1, 2]")) == NO_ERROR
    assert error_key({"MessageId": "x"}) == NO_ERROR


def test_filter_conditions_are_combined():
    m = message('{"source": "web"}', tenant="acme", ErrorMessage="Task timed out after 3.00 seconds")
    assert Filter().matches(m)
    assert not Filter()
    assert Filter(body_contains=("web",)).matches(m)
    assert not Filter(body_contains=("web", "mobile")).matches(m)
    assert Filter(attributes=(("tenant", "acme"),)).matches(m)
    assert not Filter(attributes=(("tenant", "other"),)).matches(m)
    assert not Filter(attributes=(("missing", "x"),)).matches(m)
    assert Filter(error_contains=("TIMED OUT",)).matches(m)
    assert not Filter(error_contains=("throttl",)).matches(m)
    assert not Filter(body_contains=("web",), error_contains=("throttl",)).matches(m)


def test_binary_attribute_never_equals_a_string():
    m = {"Body": "x", "MessageAttributes": {"blob": {"DataType": "Binary", "BinaryValue": b"abc"}}}
    assert not Filter(attributes=(("blob", "abc"),)).matches(m)


def test_group_example_file_by_error_and_attribute():
    messages = load_messages(EXAMPLE.read_text())
    groups = group_messages(messages, "error")
    assert [g.count for g in groups] == [4, 3, 2, 1]
    assert groups[0].key == "<time> <uuid> Task timed out after 3.00 seconds [ErrorCode 200]"
    assert groups[0].example_ids[0] in {m["MessageId"] for m in messages}
    by_code = {g.key: g.count for g in group_messages(messages, "attr:ErrorCode")}
    assert by_code == {"ErrorCode=200": 9, "ErrorCode=429": 1}
    assert group_messages(messages, "attr:nope")[0].key == "(nope not set)"
    with pytest.raises(ValueError):
        group_messages(messages, "weird")


def test_load_messages_shapes():
    assert load_messages('{"Messages": [{"Body": "a"}]}') == [{"Body": "a"}]
    assert load_messages('[{"Body": "a"}]') == [{"Body": "a"}]
    assert load_messages("{}") == []
    with pytest.raises(ValueError):
        load_messages('"text"')
