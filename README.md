# lambda-dlq-redrive

Look at what is sitting in an SQS dead-letter queue, group it by error, and move the messages you choose back to a queue or a Lambda function, at a rate you set.

**Status:** working. `peek` (group and filter without removing anything) and `redrive` (dry run by default, `--apply` to move) are implemented. Everything is tested against [moto](https://github.com/getmoto/moto)'s in-memory SQS and botocore's `Stubber`; it has not been run against a real AWS account yet. See [Limitations](#limitations).

## Install

```bash
pip install git+https://github.com/narayana006/lambda-dlq-redrive
```

Python 3.10 or newer. Installs `boto3`.

## Try it without an AWS account

`peek --from-file` reads saved `aws sqs receive-message` output instead of calling AWS. This is the real output on the sample file in this repository (ten made-up failed Lambda invocations):

```bash
lambda-dlq-redrive peek --from-file examples/dlq_messages.json --group-by error
```

```text
10 messages in examples/dlq_messages.json, 10 selected.

count  error  (example message ID)
    4  <time> <uuid> Task timed out after 3.00 seconds [ErrorCode 200]  (cf44dd3f-89e7-415f-9736-2f25244caf9c)
    3  An error occurred (ProvisionedThroughputExceededException) when calling the PutItem operation: Th...  (9d95847e-bd29-4753-a767-779673f778aa)
    2  KeyError: 'customer_id' [ErrorCode 200]  (c74c7ccf-32d0-4fdd-a123-f50190f5380e)
    1  Rate Exceeded. [ErrorCode 429]  (0d650372-e907-44df-ad52-a24135b00a54)
```

`examples/demo_moto.py` goes further: it starts moto's in-memory SQS, creates `orders-dlq` and `orders-inbox`, loads the same ten messages and runs the CLI three times. Nothing reaches AWS. Its real output (message IDs are random, so yours will differ):

```bash
pip install "moto[sqs]"
python examples/demo_moto.py
```

```text
$ lambda-dlq-redrive peek --queue orders-dlq --group-by error
Received 10 messages from https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq, 10 selected. Nothing was deleted; the messages are visible in the queue again.

count  error  (example message ID)
    4  <time> <uuid> Task timed out after 3.00 seconds [ErrorCode 200]  (e774fb71-a109-41f8-83f8-2d8087f1bfb4)
    3  An error occurred (ProvisionedThroughputExceededException) when calling the PutItem operation: Th...  (a058a812-1fb2-4f96-a860-644204fbc473)
    2  KeyError: 'customer_id' [ErrorCode 200]  (85105bfa-e853-44fc-aa17-29dd3f451379)
    1  Rate Exceeded. [ErrorCode 429]  (bd65487d-990b-4b14-a9b0-1ecfef51cce6)
[exit 0; queue depths: orders-dlq=10, orders-inbox=0]

$ lambda-dlq-redrive redrive --queue orders-dlq --target orders-inbox --error-contains 'timed out'
Dead-letter queue: https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq
Target:            queue https://sqs.us-east-1.amazonaws.com/123456789012/orders-inbox (standard)
Selection:         filtered, at most 100

  would move  e774fb71-a109-41f8-83f8-2d8087f1bfb4  <time> <uuid> Task timed out after 3.00 seconds [ErrorCod...
  would move  36a2a264-fd33-4aae-9f45-a5cc50298030  <time> <uuid> Task timed out after 3.00 seconds [ErrorCod...
  would move  b24b3c0f-e535-4445-97e1-44d2b8ae190a  <time> <uuid> Task timed out after 3.00 seconds [ErrorCod...
  would move  aa67e9df-c63b-4e5d-9043-6c933c848f99  <time> <uuid> Task timed out after 3.00 seconds [ErrorCod...

Dry run: received 10, 4 would be moved, 6 skipped by the filters.
Nothing was sent or deleted. Re-run with --apply to move them.
[exit 0; queue depths: orders-dlq=10, orders-inbox=0]

$ lambda-dlq-redrive redrive --queue orders-dlq --target orders-inbox --error-contains 'timed out' --apply --yes
Dead-letter queue: https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq
Target:            queue https://sqs.us-east-1.amazonaws.com/123456789012/orders-inbox (standard)
Selection:         filtered, at most 100

  moved       e774fb71-a109-41f8-83f8-2d8087f1bfb4 -> 49ad7148-38b2-4c61-a711-519b47555f38
  moved       36a2a264-fd33-4aae-9f45-a5cc50298030 -> 25823e7b-a84d-43ca-b56b-87c3b85a630e
  moved       b24b3c0f-e535-4445-97e1-44d2b8ae190a -> f081eb7a-575c-4243-a30a-70c18ee1c7a7
  moved       aa67e9df-c63b-4e5d-9043-6c933c848f99 -> 0f6c96eb-b59b-4c27-a552-f0ff878c0aa3

Received 10: moved 4, failed 0, sent but not deleted 0, skipped by the filters 6.
[exit 0; queue depths: orders-dlq=6, orders-inbox=4]
```

## Commands

### `peek`

```bash
lambda-dlq-redrive peek --queue orders-dlq [--group-by error|attr:NAME] [--show] [filters]
lambda-dlq-redrive peek --from-file saved.json [...]
```

Receives messages, prints them grouped, and makes them visible again. Nothing is deleted. `--show` also lists each message with its receive count and the start of its body.

The error description used for grouping comes from the message itself:

| Where the message came from | What is used |
| --- | --- |
| A Lambda function's dead-letter queue (failed asynchronous invocation) | The `ErrorMessage` and `ErrorCode` message attributes that Lambda adds |
| A Lambda on-failure destination | `responsePayload.errorType` and `errorMessage` in the body, or `requestContext.condition` |
| An SQS queue's redrive policy | Nothing: SQS moves the message as it was, so these show as `(no error information)`. Group by an attribute of your own with `--group-by attr:NAME`. |

Timestamps, UUIDs and numbers of five or more digits in the error text are replaced with `<time>`, `<uuid>` and `<n>` so that the same error with different request IDs lands in one group.

`peek` is not completely free of side effects: each received message has its `ApproximateReceiveCount` increased, and it is hidden from other consumers for the moment between the receive and the release at the end of the command (or for `--visibility-timeout` seconds, 60 by default, if the command is killed first).

### `redrive`

```bash
lambda-dlq-redrive redrive --queue orders-dlq [--target QUEUE | --target-function FUNCTION] [filters] [--apply]
```

Without `--apply` this is a dry run: it shows which messages would be moved and sends and deletes nothing. With `--apply` it asks for confirmation (type `yes`; `--yes` skips the question) and then, one message at a time:

1. sends the message to the target;
2. only after the send call has returned successfully, deletes it from the dead-letter queue.

A message is never deleted before its send succeeded.

| What happens | Result |
| --- | --- |
| Send fails | Reported as `FAILED`. The message is not deleted and is made visible in the dead-letter queue again. |
| Send succeeds, delete fails | Reported as `DUPLICATE`: the message is now in both places. It will reappear in the dead-letter queue. |
| The process is killed between send and delete | Same as above, without the report. |
| The process is killed at any other time | Nothing is lost. Messages it had received stay hidden until the visibility timeout expires. |

So delivery is at-least-once: a message can end up delivered twice, never zero times. The exit code is 1 if anything failed or was duplicated.

**Target.** `--target` takes a queue name or URL. If you give neither `--target` nor `--target-function`, the tool looks up which queue has this dead-letter queue in its redrive policy and uses it; if there is none or more than one, it stops and asks you to choose. `--target-function` invokes a Lambda function asynchronously instead (see below).

**Rate.** `--rate` is the maximum number of sends per second (default 5). Messages are sent one at a time with a pause of 1/rate seconds between sends.

**How many.** `--max N` stops after N matching messages (default 100). `--max 0` removes the limit.

### Filters (both commands)

All given filters must match.

| Option | Matches when |
| --- | --- |
| `--body-contains TEXT` | the body contains TEXT (exact, case-sensitive; repeatable) |
| `--attr NAME=VALUE` | message attribute NAME has exactly the string value VALUE (repeatable; binary attributes never match) |
| `--error-contains TEXT` | the error description shown by `peek` contains TEXT, ignoring case (repeatable) |

Messages that are received but do not match are not sent or deleted; they are made visible again when the command ends.

## Exactly what is copied

### To a queue

| Part of the message | What happens |
| --- | --- |
| Body | Copied unchanged. |
| Message attributes | All copied with the same name, data type (including custom types such as `Number.float`) and string or binary value. This includes the `RequestID`, `ErrorCode` and `ErrorMessage` attributes Lambda added. Use `--drop-attribute NAME` (repeatable) to leave one out. |
| System attributes (`SentTimestamp`, `ApproximateReceiveCount`, `SenderId`, `AWSTraceHeader`, ...) | Not copied. The target receives a new message: new `MessageId`, new sent time, receive count starting again, and the sender is whoever runs this tool. |
| `MessageGroupId`, FIFO target | The original message's group ID is reused, so messages stay in their group. If the message has none (a standard dead-letter queue feeding a FIFO target) the send fails with a clear error unless you pass `--message-group-id ID`, which is then used for every message. |
| `MessageDeduplicationId`, FIFO target | **Never reused.** Every send gets a new random ID, which also takes precedence over content-based deduplication. The reason: if the original ID were reused within the FIFO deduplication interval (5 minutes), SQS would accept the send and silently drop the message, and the tool would then delete the only remaining copy. |
| `MessageGroupId` / `MessageDeduplicationId`, standard target | Not sent. |

**FIFO ordering.** Messages are sent in the order SQS returns them, which for a FIFO dead-letter queue is the order within each group. If a send fails, the remaining messages of that group are not sent in this run (they are reported as failed and stay in the dead-letter queue), so a later message cannot overtake an earlier one. Other groups continue.

**FIFO dead-letter queues and filters.** SQS does not return further messages of a message group while earlier ones from that group are still in flight. `peek`, a dry run, and any message skipped by a filter hold messages in flight until the command ends, so on a FIFO dead-letter queue they can only see as far into each group as SQS hands out before blocking it, and a skipped message keeps the messages behind it in its group out of reach for that run.

### To a function (`--target-function`)

The message body is sent as the event of an asynchronous invocation (`InvocationType=Event`). The send counts as successful when Lambda answers with HTTP 202, which means Lambda accepted the event, not that the function processed it; if it fails again, Lambda's own retry and dead-letter settings apply. Message attributes are not passed to the function. A body that is not valid JSON is rejected by Lambda and the message stays in the dead-letter queue.

This fits a function's own dead-letter queue, where the body is the original event. For messages written by an on-failure destination the body is the destination record that wraps the original event, and it is sent as is.

## IAM permissions

| Action | On | Needed for |
| --- | --- | --- |
| `sqs:GetQueueUrl` | both queues | when you pass a queue name rather than a URL |
| `sqs:ReceiveMessage` | dead-letter queue | `peek`, `redrive` (also the dry run) |
| `sqs:ChangeMessageVisibility` | dead-letter queue | making received messages visible again |
| `sqs:ListDeadLetterSourceQueues` | dead-letter queue | `redrive` without `--target` or `--target-function` |
| `sqs:SendMessage` | target queue | `redrive --apply` to a queue |
| `sqs:DeleteMessage` | dead-letter queue | `redrive --apply` |
| `lambda:InvokeFunction` | target function | `redrive --apply --target-function` |

For queues encrypted with a customer managed KMS key you also need `kms:Decrypt` on the dead-letter queue's key, and `kms:GenerateDataKey` and `kms:Decrypt` on the target queue's key.

`peek --from-file` needs no permissions and makes no AWS calls.

## Options reference

```text
peek     --queue NAME_OR_URL | --from-file PATH
         --group-by error|attr:NAME   (default error)
         --show
redrive  --queue NAME_OR_URL
         --target NAME_OR_URL | --target-function NAME
         --apply  --yes  --rate PER_SECOND (default 5)
         --message-group-id ID  --drop-attribute NAME
both     --body-contains TEXT  --attr NAME=VALUE  --error-contains TEXT
         --max N (default 100, 0 = no limit)
         --visibility-timeout SECONDS (default 60)  --wait SECONDS (default 1)
         --region REGION  --profile PROFILE
```

## Features

- [x] Peek at messages without removing them, from a queue or from a saved file
- [x] Group failures by error, or by any message attribute
- [x] Filter by attribute, body content or error text
- [x] Replay to a queue (explicit, or the source queue found automatically) or to a function, at a rate limit
- [x] Dry run by default, `--apply` plus a confirmation before anything is moved
- [x] FIFO queues: group ID kept, new deduplication ID, order kept within a group
- [ ] Unwrap on-failure destination records and replay only the original event
- [ ] Failure records from stream event source mappings (Kinesis, DynamoDB Streams)
- [ ] JSON output and saving peeked messages to a file
- [ ] Batched sends for higher throughput

## Limitations

- Tested against moto and `Stubber` only. moto follows SQS behaviour closely for what is used here (visibility, FIFO groups, deduplication of a message still in the queue), but it is an emulator; try a dry run and a small `--max` on a real queue first.
- If you simply want every message moved back, SQS has its own dead-letter queue redrive (the console's "Start DLQ redrive", `StartMessageMoveTask` in the API) that does it on the server. This tool is for looking first and moving a chosen subset.
- Delivery is at-least-once, as described above. Consumers of the target should tolerate a duplicate.
- A run that takes longer than `--visibility-timeout` lets skipped and failed messages become visible again before it ends. They are recognised by message ID and not processed twice in the same run, but other consumers can see them.
- Sends are one message per API call, so throughput is modest by design.
- Error grouping depends on what the message carries; messages moved by an SQS redrive policy have no error information.

## Development

```bash
git clone https://github.com/narayana006/lambda-dlq-redrive
cd lambda-dlq-redrive
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest
```

The tests set dummy credentials and run entirely inside moto.

## Contributing

Issues and pull requests are welcome, especially reports from real queues where the behaviour differs from what is written here.

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
