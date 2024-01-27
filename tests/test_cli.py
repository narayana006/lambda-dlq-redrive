import io
from pathlib import Path

import boto3
import pytest

from conftest import lambda_failure, visible_count
from lambda_dlq_redrive.cli import main

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "dlq_messages.json"


def run(argv, answer=None):
    out, err = io.StringIO(), io.StringIO()
    asked = []

    def ask(prompt):
        asked.append(prompt)
        return answer

    live = bool({"--queue"} & set(argv))
    code = main(argv + (["--wait", "0"] if live else []),
                clients=lambda service, region, profile: boto3.client(service, region_name="us-east-1"),
                sleep=lambda seconds: None, ask=ask, out=out, err=err)
    return code, out.getvalue(), err.getvalue(), asked


@pytest.fixture
def loaded(sqs):
    dlq = sqs.create_queue(QueueName="orders-dlq")["QueueUrl"]
    target = sqs.create_queue(QueueName="orders-inbox")["QueueUrl"]
    for index in range(3):
        lambda_failure(sqs, dlq, f'{{"n": {index}, "source": "web"}}')
    for index in range(2):
        lambda_failure(sqs, dlq, f'{{"k": {index}, "source": "mobile"}}', error="KeyError: 'customer_id'")
    return dlq, target


def test_peek_from_file_needs_no_aws():
    code, out, _, _ = run(["peek", "--from-file", str(EXAMPLE), "--group-by", "error"])
    assert code == 0
    assert "10 messages in" in out and "10 selected" in out
    lines = out.splitlines()
    assert lines[3].lstrip().startswith("4  <time> <uuid> Task timed out after 3.00 seconds [ErrorCode 200]")


def test_peek_from_file_with_filter_show_and_attribute_grouping():
    code, out, _, _ = run(["peek", "--from-file", str(EXAMPLE), "--attr", "ErrorCode=429",
                           "--group-by", "attr:ErrorCode", "--show"])
    assert code == 0
    assert "1 selected" in out
    assert "ErrorCode=429" in out
    assert "receives=1" in out


def test_peek_from_file_errors(tmp_path):
    assert run(["peek", "--from-file", str(tmp_path / "missing.json")])[0] == 2
    bad = tmp_path / "bad.json"
    bad.write_text("not json")
    assert run(["peek", "--from-file", str(bad)])[0] == 2


def test_peek_queue_groups_and_leaves_queue_untouched(sqs, loaded):
    dlq, _ = loaded
    code, out, _, _ = run(["peek", "--queue", "orders-dlq"])
    assert code == 0
    assert "Received 5 messages" in out and "Nothing was deleted" in out
    assert "    3  Task timed out after 3.00 seconds [ErrorCode 200]" in out
    assert "    2  KeyError: 'customer_id' [ErrorCode 200]" in out
    assert visible_count(sqs, dlq) == (5, 0)


def test_redrive_is_a_dry_run_by_default(sqs, loaded):
    dlq, target = loaded
    code, out, _, asked = run(["redrive", "--queue", "orders-dlq", "--target", "orders-inbox"])
    assert code == 0
    assert out.count("would move") == 5
    assert "Dry run: received 5, 5 would be moved, 0 skipped" in out
    assert "Nothing was sent or deleted" in out
    assert asked == []
    assert visible_count(sqs, dlq) == (5, 0)
    assert visible_count(sqs, target) == (0, 0)


def test_apply_asks_first_and_anything_but_yes_cancels(sqs, loaded):
    dlq, target = loaded
    code, out, _, asked = run(["redrive", "--queue", "orders-dlq", "--target", "orders-inbox", "--apply"], answer="y")
    assert code == 1
    assert len(asked) == 1 and "Type 'yes'" in asked[0]
    assert "Cancelled" in out
    assert visible_count(sqs, dlq) == (5, 0)
    assert visible_count(sqs, target) == (0, 0)


def test_apply_with_confirmation_moves_filtered_messages(sqs, loaded):
    dlq, target = loaded
    code, out, _, _ = run(["redrive", "--queue", "orders-dlq", "--target", "orders-inbox", "--apply",
                           "--body-contains", '"source": "web"'], answer="yes")
    assert code == 0
    assert out.count("  moved ") == 3
    assert "Received 5: moved 3, failed 0, sent but not deleted 0, skipped by the filters 2." in out
    assert visible_count(sqs, dlq) == (2, 0)
    assert visible_count(sqs, target) == (3, 0)


def test_target_defaults_to_the_source_queue(sqs):
    dlq = sqs.create_queue(QueueName="jobs-dlq")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    source = sqs.create_queue(QueueName="jobs", Attributes={
        "RedrivePolicy": '{"deadLetterTargetArn": "%s", "maxReceiveCount": "3"}' % arn})["QueueUrl"]
    sqs.send_message(QueueUrl=dlq, MessageBody="job-1")
    code, out, _, _ = run(["redrive", "--queue", "jobs-dlq", "--apply", "--yes"])
    assert code == 0
    assert "found through the dead-letter queue's source queues" in out
    assert visible_count(sqs, source) == (1, 0)
    assert visible_count(sqs, dlq) == (0, 0)


def test_no_target_and_no_source_queue_is_an_error(sqs, loaded):
    code, _, err, _ = run(["redrive", "--queue", "orders-dlq", "--apply", "--yes"])
    assert code == 1
    assert "pass --target or --target-function" in err
    assert visible_count(sqs, loaded[0]) == (5, 0)


def test_target_must_differ_from_the_dlq(sqs, loaded):
    code, _, err, _ = run(["redrive", "--queue", "orders-dlq", "--target", "orders-dlq", "--apply", "--yes"])
    assert code == 1 and "dead-letter queue itself" in err


def test_failures_give_exit_code_1_and_keep_messages(sqs, loaded):
    dlq, _ = loaded
    code, out, _, _ = run(["redrive", "--queue", "orders-dlq", "--apply", "--yes", "--target",
                           "https://sqs.us-east-1.amazonaws.com/123456789012/gone"])
    assert code == 1
    assert out.count("FAILED") == 5 and "left in the dead-letter queue" in out
    assert "moved 0, failed 5" in out
    assert visible_count(sqs, dlq) == (5, 0)


def test_unknown_queue_and_bad_arguments(sqs):
    code, _, err, _ = run(["peek", "--queue", "nope"])
    assert code == 1 and "error:" in err
    assert run(["peek", "--queue", "x", "--max", "-1"])[0] == 2
    assert run(["redrive", "--queue", "x", "--rate", "0"])[0] == 2
