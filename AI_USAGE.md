# AI Usage Disclosure

## Tools used

ChatGPT with Codex and the GitHub connector.

## What I used them for

Codex reviewed the starter brief, proposed the SQLite design and implemented the pipeline,
tests and documentation. It ran the tests, compared report outputs across ingest strategies
and measured the large-data run.

## Candidate decisions and contribution

Manivasagam selected Assignment A and requested a technical walkthrough for the interview.
The implementation and verification in this submission were performed with Codex. Candidate
review or authorship beyond that selection is not claimed.

## Where the AI was wrong or unhelpful

The first benchmark command assumed `/usr/bin/time` was installed. The command failed before
running the pipeline, so Codex reran the benchmark with the shell's timer and recorded those
measured results. The first decimal formatter also relied on the default precision when
rounding. Codex revised it to scale precision with the input.

## How I verified AI-generated code

Codex ran five tests with expected report values. It also generated independent synthetic seeds
and checked that sequential imports, replays and a combined import yielded identical output.
The large run exercised the same command-line entry points.
