# BetStream

Real time fraud detection on synthetic iGaming betting events. Kafka feeds
Spark Structured Streaming, which lands events through Bronze and Silver into
windowed per player features, then applies two fraud rules. Detections are
scored against a ground truth ledger the pipeline never reads during
processing.

Built as a portfolio project covering the streaming half of a data
engineering stack. A companion project covers batch data quality on mining
telemetry.

## What it does

The generator creates betting events for 300 players over 7 days, 5% of them
fraudulent, and writes a separate ledger recording who is who. The pipeline
never sees that ledger while running. It infers fraud from signals only:
device ids, payment methods, timestamps, deposit and wager and withdrawal
amounts.

Two fraud patterns are detected:

**Suspicious flow**, a money movement signal. A player deposits a large
amount, wagers almost none of it, then withdraws quickly. Detected with a
stateless rule over windowed features.

**Bonus abuse**, an account linkage signal. Many accounts share one device to
farm signup bonuses. Detected with a stateful cross key aggregation that
never forgets, since accounts can be linked to a device at any time.

The two are deliberately separate problems. Bonus abuse players behave
normally on every money metric, so no behavioural rule reaches them, which is
why the second rule exists rather than another condition on the first.

## Pipeline stages

1. **Generator** writes synthetic events and the ground truth ledger.
2. **Producer** replays the events into a Kafka topic.
3. **Bronze** reads Kafka, lands raw events into Delta, append only.
4. **Silver** validates and deduplicates with a 15 minute watermark; invalid
   rows go to a quarantine table.
5. **Features** windows Silver into per player deposit, wager, and
   withdrawal totals.
6. **M5** applies the suspicious flow rule to the windowed features.
7. **M6** applies the bonus abuse rule directly to Silver, tracking device to
   account linkage statefully.
8. **Evaluation** scores both rules' output against the ground truth ledger.

## Results

| rule | tp | fp | fn | precision | recall |
|------|----|----|----|-----------|--------|
| wager_ratio alone | 7 | 181 | 0 | 0.04 | 1.00 |
| withdrawal_ratio alone | 7 | 6 | 0 | 0.54 | 1.00 |
| M5 combined | 7 | 4 | 0 | 0.64 | 1.00 |
| M6 bonus abuse | 5 | 0 | 3 | 1.00 | 0.62 |
| pipeline combined | 12 | 4 | 3 | 0.75 | 0.80 |

M5 catches every real case, with just over a third of its alerts being false
positives. The ablation is the more useful result: wager ratio alone produces
181 false positives and is close to worthless by itself, withdrawal ratio
alone produces 6, and the two conditions together bring it down to 4. Two
mediocre signals combined beat the stronger single signal.

M6 shows perfect precision, but that number is flattered by the data. The
generator injects no legitimate device sharing anywhere, so there is nothing
innocent for the rule to trip over. Recall of 0.62 is by design: the
threshold is 5 accounts per device, and one cluster sits at 3, so it is
missed, as predicted in writing before the evaluation ran.

Full writeup with limitations: [reports/m7_evaluation.md](reports/m7_evaluation.md)

## Running it

Requires Docker, Python 3.11, and a JDK on PATH. First Spark run downloads
Delta and Kafka connector jars, so it needs internet. All commands from the
repo root.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d                        # Kafka, plus Kafka UI on :8080

python generator/generate_data.py           # events.jsonl + ground_truth.csv
python producers/produce_events.py          # replays into betstream.events

python streaming/m2_bronze.py               # -> data/delta/bronze
python streaming/m3_silver.py               # -> data/delta/silver, quarantine
python streaming/m4_windows.py              # -> data/delta/features_windowed
python streaming/m5_suspicious_flow.py      # -> data/delta/flagged_suspicious_flow
python streaming/m6_bonus_abuse.py          # -> data/delta/flagged_devices
python batch/m7_evaluation.py               # -> data/delta/evaluation_metrics
```

Steps run strictly in order, none concurrently. Every streaming job uses an
`availableNow` trigger, so each processes what's available and exits, rather
than running continuously. Only Kafka needs to stay up, and only through the
Bronze step.

Tests need no data or Kafka:

```bash
python -m pytest tests/ -v
```

`streaming/m1_kafka_to_console.py` is a demo that runs until Ctrl-C. It is
not part of the persisted pipeline.

### Rerunning

Every streaming step is checkpointed, so rerunning with no new data does
nothing. To start clean:

```bash
rm -rf data/
docker compose down -v
```

Without `down -v` the Kafka volume survives, and Bronze reading from
earliest offsets will reingest old topic contents alongside new ones.

If you regenerate Silver only, delete `data/delta/_flush_source` and
`data/checkpoints/features_windowed` before rerunning M4, or the flush
timestamp that forces the final windows out will be stale.

## Known limitations

**M5 flags windows, not cases.** Multiple windows can flag the same player.
Detections are collapsed to distinct players before scoring, but a proper
entity level case table with a risk score belongs downstream and doesn't
exist here yet.

**Difficulty is self set.** The same person wrote the generator and the
rules. Thresholds were chosen before looking at labels, but the data was
built knowing what the rules would look for.

**State grows without bound.** M6 deliberately never expires state, so it
can link accounts added to a device at any point in time. Fine at 294
devices, not fine at millions. A production version needs a state TTL and an
explicit decision about how far back linkage should reach.

**300 players is small.** Single players move the metrics visibly.

## Production pattern

This runs entirely local: Kafka in Docker, Spark native in a venv, Delta on
the local filesystem. The production version of the same design is Kafka
into an object storage sink, then a lakehouse on top, with the streaming
jobs running as managed compute rather than a laptop process. The Spark and
Delta API is the same either way, so the local version is a fair proxy for
the logic, though not for the operations.

What a production deployment would add that this doesn't have: monitoring on
batch duration, input rate, state store size, and watermark lag; backpressure
limits; schema evolution handling at the Bronze boundary; and a cost model
for the compute.

## CI

GitHub Actions runs on every push: installs pinned dependencies on a clean
machine, compiles every Python file, and runs the test suite. The clean
machine matters as much as the tests, since it proves the project stands up
from `requirements.txt` alone rather than from anything undocumented sitting
on a laptop.

Two tests cover the scoring function: one for the arithmetic, one that locks
in a judgement call, that a flagged player of a different fraud type still
counts as a false positive.

## Roadmap

- AML pattern detection beyond the single suspicious flow signal
- Personalisation features off the same event stream
- An entity level case table, one row per flagged player with a risk score,
  rather than window level alerts
- Legitimate device sharing in the generator, so M6 precision means something
- Schema evolution handling at the Bronze boundary
- Scale test at 1M events

## Repo layout

- `generator/` synthetic players, fraud scenarios, ground truth ledger
- `producers/` replays generated events into Kafka
- `streaming/` Spark Structured Streaming jobs, M1 through M6
- `batch/` evaluation and scoring
- `scripts/` manual tools, not tests
- `tests/` pytest suite
- `reports/` milestone writeups