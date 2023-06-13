# lambda-dlq-redrive

Inspect, filter and safely replay failed events from AWS Lambda dead-letter queues.

> **Project status: planning. There is no working code in this repository yet.**
> This README describes what the project will do. Nothing below can be installed or run today.
> Features are ticked off in the checklist as they are built.

## What it is

A command-line tool.

**The problem it solves:** Failed events sit in dead-letter queues because looking at them and replaying them safely is awkward.

## Who it is for

Developers running event-driven workloads on Lambda and SQS.

## How it will work

1. You point the tool at a dead-letter queue.
2. It shows what failed and why, grouped by error.
3. You replay the messages you choose, at a safe rate.

## Planned features

Unticked items are not built yet.

- [ ] Peek at messages without removing them
- [ ] Group failures by error type
- [ ] Filter by attribute or body content
- [ ] Replay to the original function or queue at a rate limit
- [ ] Dry-run and a confirmation before anything is replayed

## Planned usage

This is the intended interface, shown so you can judge the design. **It does not work yet**, and names may change before the first release.

```bash
lambda-dlq-redrive peek --queue orders-dlq --group-by error
```

## Contributing

Feedback on the design is the most useful contribution right now. Open an issue to suggest a feature, question an approach or describe your use case. Code contributions are welcome once the first version lands.

## Author

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
