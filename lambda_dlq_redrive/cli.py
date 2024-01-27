"""Command line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from botocore.exceptions import BotoCoreError, ClientError

from . import __version__
from .messages import Filter, Message, error_key, group_messages, load_messages
from .queue import (FunctionTarget, Outcome, QueueTarget, RedriveError, find_source_queue, process,
                    resolve_queue_url)

ClientFactory = Callable[[str, Optional[str], Optional[str]], Any]


def default_client_factory(service: str, region: Optional[str], profile: Optional[str]) -> Any:
    import boto3
    return boto3.Session(region_name=region, profile_name=profile).client(service)


def _attribute_pair(argument: str) -> tuple[str, str]:
    name, separator, value = argument.partition("=")
    if not separator or not name:
        raise argparse.ArgumentTypeError("expected NAME=VALUE")
    return name, value


def _group_by(argument: str) -> str:
    if argument == "error" or (argument.startswith("attr:") and len(argument) > 5):
        return argument
    raise argparse.ArgumentTypeError("expected 'error' or 'attr:NAME'")


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--body-contains", action="append", default=[], metavar="TEXT",
                        help="only messages whose body contains TEXT (repeatable, all must match)")
    parser.add_argument("--attr", action="append", default=[], type=_attribute_pair, metavar="NAME=VALUE",
                        help="only messages whose message attribute NAME equals VALUE (repeatable)")
    parser.add_argument("--error-contains", action="append", default=[], metavar="TEXT",
                        help="only messages whose error description contains TEXT, ignoring case (repeatable)")
    parser.add_argument("--max", type=int, default=100, dest="max_messages", metavar="N",
                        help="stop after N matching messages; 0 means no limit (default 100)")
    parser.add_argument("--visibility-timeout", type=int, default=60, metavar="SECONDS",
                        help="how long received messages stay hidden from other consumers while the "
                             "command works (default 60)")
    parser.add_argument("--wait", type=int, default=1, metavar="SECONDS",
                        help="long-poll wait per receive call, 0-20 (default 1)")
    parser.add_argument("--region", help="AWS region (default: from your AWS configuration)")
    parser.add_argument("--profile", help="AWS profile name (default: default credential chain)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lambda-dlq-redrive",
        description="Inspect, filter and replay messages from an SQS dead-letter queue.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    peek = commands.add_parser("peek", help="show what is in a dead-letter queue without removing anything")
    source = peek.add_mutually_exclusive_group(required=True)
    source.add_argument("--queue", help="dead-letter queue name or URL")
    source.add_argument("--from-file", metavar="PATH",
                        help="read messages from saved `aws sqs receive-message` JSON instead of AWS")
    peek.add_argument("--group-by", type=_group_by, default="error", metavar="error|attr:NAME",
                      help="group by error description (default) or by a message attribute")
    peek.add_argument("--show", action="store_true", help="also list each matching message")
    _add_common(peek)

    redrive = commands.add_parser(
        "redrive", help="move matching messages to a queue or function (dry run unless --apply)")
    redrive.add_argument("--queue", required=True, help="dead-letter queue name or URL")
    target = redrive.add_mutually_exclusive_group()
    target.add_argument("--target", metavar="QUEUE",
                        help="queue name or URL to send to (default: the queue whose redrive policy "
                             "points at the dead-letter queue)")
    target.add_argument("--target-function", metavar="FUNCTION",
                        help="invoke this Lambda function asynchronously with each message body")
    redrive.add_argument("--apply", action="store_true", help="really send and delete; without it nothing is moved")
    redrive.add_argument("--yes", action="store_true", help="with --apply, do not ask for confirmation")
    redrive.add_argument("--rate", type=float, default=5.0, metavar="PER_SECOND",
                         help="maximum messages sent per second (default 5)")
    redrive.add_argument("--message-group-id", metavar="ID",
                         help="MessageGroupId to use for a FIFO target instead of each message's own")
    redrive.add_argument("--drop-attribute", action="append", default=[], metavar="NAME",
                         help="do not copy this message attribute to the target queue (repeatable)")
    _add_common(redrive)
    return parser


def _filter(args) -> Filter:
    return Filter(tuple(args.body_contains), tuple(args.attr), tuple(args.error_contains))


def _one_line(text: str, width: int = 70) -> str:
    text = " ".join(text.split())
    return text if len(text) <= width else text[:width - 3] + "..."


def _print_groups(messages: Sequence[Message], group_by: str, out) -> None:
    groups = group_messages(messages, group_by)
    width = max([5] + [len(str(g.count)) for g in groups])
    print(f"{'count'.rjust(width)}  {'error' if group_by == 'error' else group_by}  (example message ID)", file=out)
    for group in groups:
        print(f"{str(group.count).rjust(width)}  {_one_line(group.key, 100)}  ({group.example_ids[0]})", file=out)


def _print_messages(messages: Sequence[Message], out) -> None:
    for message in messages:
        attributes = message.get("Attributes") or {}
        receives = attributes.get("ApproximateReceiveCount", "?")
        print(f"  {message.get('MessageId', '?')}  receives={receives}  {_one_line(message.get('Body') or '')}", file=out)


def _peek(args, clients: ClientFactory, sleep, out, err) -> int:
    message_filter = _filter(args)
    if args.from_file:
        try:
            messages = load_messages(Path(args.from_file).read_text(encoding="utf-8"))
        except OSError as error:
            print(f"error: cannot read {args.from_file}: {error.strerror}", file=err)
            return 2
        except ValueError as error:
            print(f"error: {args.from_file}: {error}", file=err)
            return 2
        matching = [m for m in messages if message_filter.matches(m)]
        if args.max_messages:
            matching = matching[:args.max_messages]
        print(f"{len(messages)} messages in {args.from_file}, {len(matching)} selected.", file=out)
    else:
        sqs = clients("sqs", args.region, args.profile)
        url = resolve_queue_url(sqs, args.queue)
        result = process(sqs, url, message_filter, max_messages=args.max_messages,
                         visibility_timeout=args.visibility_timeout, wait_seconds=args.wait, sleep=sleep)
        matching = result.matched_messages
        print(f"Received {result.received} messages from {url}, {len(matching)} selected. "
              "Nothing was deleted; the messages are visible in the queue again.", file=out)
        if result.release_failures:
            print(f"warning: {result.release_failures} message(s) could not be made visible again and "
                  f"stay hidden for up to {args.visibility_timeout} seconds.", file=err)
    if matching:
        print("", file=out)
        _print_groups(matching, args.group_by, out)
        if args.show:
            print("", file=out)
            _print_messages(matching, out)
    return 0


def _redrive(args, clients: ClientFactory, sleep, ask, out, err) -> int:
    if args.rate <= 0:
        print("error: --rate must be greater than 0", file=err)
        return 2
    sqs = clients("sqs", args.region, args.profile)
    dlq_url = resolve_queue_url(sqs, args.queue)
    if args.target_function:
        target = FunctionTarget(clients("lambda", args.region, args.profile), args.target_function)
    else:
        found = ""
        if args.target:
            target_url = resolve_queue_url(sqs, args.target)
        else:
            target_url = find_source_queue(sqs, dlq_url)
            found = " (found through the dead-letter queue's source queues)"
        if target_url == dlq_url:
            raise RedriveError("the target is the dead-letter queue itself")
        target = QueueTarget(sqs, target_url, args.message_group_id, args.drop_attribute)
        target.description += found
    message_filter = _filter(args)
    limit = "no limit" if args.max_messages == 0 else f"at most {args.max_messages}"
    print(f"Dead-letter queue: {dlq_url}", file=out)
    print(f"Target:            {target.description}", file=out)
    print(f"Selection:         {'filtered' if message_filter else 'all messages'}, {limit}", file=out)

    if args.apply and not args.yes:
        answer = ask(f"Move matching messages to the target at up to {args.rate:g} per second? "
                     "Type 'yes' to continue: ")
        if answer.strip().lower() != "yes":
            print("Cancelled. Nothing was sent or deleted.", file=out)
            return 1
    print("", file=out)

    def show(outcome: Outcome, message: Message) -> None:
        if outcome.status == "moved":
            print(f"  moved       {outcome.message_id} -> {outcome.detail}", file=out)
        elif outcome.status == "would-move":
            print(f"  would move  {outcome.message_id}  {_one_line(outcome.detail, 60)}", file=out)
        elif outcome.status == "failed":
            print(f"  FAILED      {outcome.message_id}  {outcome.detail} (left in the dead-letter queue)", file=out)
        elif outcome.status == "sent-not-deleted":
            print(f"  DUPLICATE   {outcome.message_id}  {outcome.detail} (now in both places)", file=out)

    result = process(sqs, dlq_url, message_filter, target, apply=args.apply,
                     max_messages=args.max_messages, rate=args.rate,
                     visibility_timeout=args.visibility_timeout, wait_seconds=args.wait,
                     sleep=sleep, on_outcome=show)
    skipped = result.count("skipped")
    print("", file=out)
    if not args.apply:
        print(f"Dry run: received {result.received}, {result.count('would-move')} would be moved, "
              f"{skipped} skipped by the filters.", file=out)
        print("Nothing was sent or deleted. Re-run with --apply to move them.", file=out)
    else:
        failed, duplicated = result.count("failed"), result.count("sent-not-deleted")
        print(f"Received {result.received}: moved {result.count('moved')}, failed {failed}, "
              f"sent but not deleted {duplicated}, skipped by the filters {skipped}.", file=out)
    if result.release_failures:
        print(f"warning: {result.release_failures} message(s) could not be made visible again and stay "
              f"hidden for up to {args.visibility_timeout} seconds.", file=err)
    if args.apply and (result.count("failed") or result.count("sent-not-deleted")):
        return 1
    return 0


def main(argv: Optional[Sequence[str]] = None, clients: ClientFactory = default_client_factory,
         sleep: Optional[Callable[[float], None]] = None, ask: Callable[[str], str] = input,
         out=None, err=None) -> int:
    out = out or sys.stdout
    err = err or sys.stderr
    args = build_parser().parse_args(argv)
    if sleep is None:
        import time
        sleep = time.sleep
    problems = []
    if args.max_messages < 0:
        problems.append("--max cannot be negative")
    if not 0 <= args.wait <= 20:
        problems.append("--wait must be between 0 and 20")
    if not 1 <= args.visibility_timeout <= 43200:
        problems.append("--visibility-timeout must be between 1 and 43200")
    if problems:
        for problem in problems:
            print(f"error: {problem}", file=err)
        return 2
    try:
        if args.command == "peek":
            return _peek(args, clients, sleep, out, err)
        return _redrive(args, clients, sleep, ask, out, err)
    except (RedriveError, ClientError, BotoCoreError) as error:
        print(f"error: {error}", file=err)
        return 1


if __name__ == "__main__":
    sys.exit(main())
