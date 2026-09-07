"""
Generates synthetic BetStream events + a ground-truth fraud ledger.

Output:
  data/raw/events.jsonl        - one JSON event per line, chronological by event_ts
  data/raw/ground_truth.csv    - player_id, fraud_type (null | suspicious_flow | bonus_abuse)

The pipeline must NEVER read ground_truth.csv. It exists only for M7 evaluation.
"""
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
from config import GenConfig

fake = Faker()


def make_players(cfg: GenConfig, rng: random.Random):
    """Assigns each player an id, device_id, payment_method_id, and a behavior profile."""
    n_fraud = int(cfg.n_players * cfg.fraud_pct)
    n_suspicious = n_fraud // 2
    n_bonus = n_fraud - n_suspicious
    n_normal = cfg.n_players - n_fraud
    n_high_roller = int(n_normal * cfg.high_roller_pct)

    players = []

    # normal players (most with modest, unremarkable behavior)
    for _ in range(n_normal - n_high_roller):
        players.append({
            "player_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "payment_method_id": str(uuid.uuid4()),
            "profile": "normal",
            "fraud_type": None,
        })

    # normal but high-roller: big deposits, fast cashout, but NOT fraud.
    # this is your overlap noise against suspicious_flow — don't skip it.
    for _ in range(n_high_roller):
        players.append({
            "player_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "payment_method_id": str(uuid.uuid4()),
            "profile": "high_roller",
            "fraud_type": None,
        })

    # suspicious_flow fraud — some extreme, some subtle (subtle overlaps high_roller)
    for i in range(n_suspicious):
        subtle = rng.random() < cfg.subtle_fraud_pct
        players.append({
            "player_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "payment_method_id": str(uuid.uuid4()),
            "profile": "suspicious_flow_subtle" if subtle else "suspicious_flow",
            "fraud_type": "suspicious_flow",
        })

    # bonus_abuse fraud — clustered by shared device_id
    remaining = n_bonus
    while remaining > 0:
        cluster_size = min(remaining, rng.randint(*cfg.bonus_abuse_cluster_size))
        shared_device = str(uuid.uuid4())
        for _ in range(cluster_size):
            players.append({
                "player_id": str(uuid.uuid4()),
                "device_id": shared_device,          # <-- the signal
                "payment_method_id": str(uuid.uuid4()),
                "profile": "bonus_abuse",
                "fraud_type": "bonus_abuse",
            })
        remaining -= cluster_size

    rng.shuffle(players)
    return players


def gen_events_for_player(player, cfg: GenConfig, rng: random.Random, start_ts: datetime):
    """Returns a list of event dicts for one player, timestamps within cfg.days_span."""
    events = []
    active_start = start_ts + timedelta(hours=rng.uniform(0, cfg.days_span * 24 * 0.7))
    t = active_start

    def emit(event_type, amount=None):
        nonlocal t
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "player_id": player["player_id"],
            "device_id": player["device_id"],
            "payment_method_id": player["payment_method_id"],
            "amount": amount,
            "event_ts": t.isoformat(),
        })

    emit("player_created")
    t += timedelta(minutes=rng.uniform(5, 120))
    emit("login")

    profile = player["profile"]

    if profile == "normal":
        for _ in range(rng.randint(2, 6)):
            t += timedelta(hours=rng.uniform(1, 48))
            emit("deposit", round(rng.uniform(20, 300), 2))
            t += timedelta(minutes=rng.uniform(10, 600))
            emit("bet_placed", round(rng.uniform(5, 100), 2))
            if rng.random() < 0.4:
                t += timedelta(hours=rng.uniform(1, 72))
                emit("withdrawal", round(rng.uniform(10, 200), 2))

    elif profile == "high_roller":
        # big deposit, fast cashout — legit, but shaped like fraud on purpose
        for _ in range(rng.randint(1, 3)):
            t += timedelta(hours=rng.uniform(1, 24))
            deposit_amt = round(rng.uniform(2000, 8000), 2)
            emit("deposit", deposit_amt)
            t += timedelta(minutes=rng.uniform(20, 90))
            emit("bet_placed", round(deposit_amt * rng.uniform(0.3, 0.9), 2))
            t += timedelta(minutes=rng.uniform(15, 60))
            emit("withdrawal", round(deposit_amt * rng.uniform(0.5, 1.1), 2))

    elif profile in ("suspicious_flow", "suspicious_flow_subtle"):
        subtle = profile.endswith("subtle")
        for _ in range(rng.randint(1, 2)):
            t += timedelta(hours=rng.uniform(1, 24))
            deposit_amt = round(rng.uniform(1500, 6000), 2)
            emit("deposit", deposit_amt)
            # low wagering relative to deposit — the actual signal
            wager_frac = rng.uniform(0.15, 0.35) if subtle else rng.uniform(0.01, 0.08)
            t += timedelta(minutes=rng.uniform(5, 30) if not subtle else rng.uniform(20, 60))
            emit("bet_placed", round(deposit_amt * wager_frac, 2))
            # fast withdrawal — subtle version waits a bit longer
            t += timedelta(minutes=rng.uniform(5, 20) if not subtle else rng.uniform(30, 90))
            emit("withdrawal", round(deposit_amt * rng.uniform(0.85, 1.0), 2))

    elif profile == "bonus_abuse":
        # behaviorally unremarkable — the tell is the shared device_id, not the amounts
        for _ in range(rng.randint(2, 4)):
            t += timedelta(hours=rng.uniform(1, 48))
            emit("deposit", round(rng.uniform(20, 150), 2))
            t += timedelta(minutes=rng.uniform(10, 300))
            emit("bet_placed", round(rng.uniform(5, 50), 2))
            if rng.random() < 0.5:
                t += timedelta(hours=rng.uniform(1, 48))
                emit("withdrawal", round(rng.uniform(10, 100), 2))

    return events


def main():
    cfg = GenConfig()
    rng = random.Random(cfg.seed)
    Faker.seed(cfg.seed)

    out_dir = Path(cfg.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    players = make_players(cfg, rng)
    start_ts = datetime.utcnow() - timedelta(days=cfg.days_span)

    all_events = []
    for p in players:
        all_events.extend(gen_events_for_player(p, cfg, rng, start_ts))

    # optional duplicate injection — off by default, flip on for M3
    if cfg.inject_duplicates:
        dupes = rng.sample(all_events, k=int(len(all_events) * 0.01))
        all_events.extend(dupes)

    # optional late-event injection — off by default, flip on for M3
    if cfg.inject_late_pct > 0:
        n_late = int(len(all_events) * cfg.inject_late_pct)
        for e in rng.sample(all_events, k=n_late):
            original = datetime.fromisoformat(e["event_ts"])
            e["event_ts"] = (original - timedelta(hours=rng.uniform(1, 6))).isoformat()

    all_events.sort(key=lambda e: e["event_ts"])

    events_path = out_dir / "events.jsonl"
    with open(events_path, "w") as f:
        for e in all_events:
            f.write(json.dumps(e) + "\n")

    ledger_path = out_dir / "ground_truth.csv"
    with open(ledger_path, "w") as f:
        f.write("player_id,fraud_type\n")
        for p in players:
            f.write(f"{p['player_id']},{p['fraud_type'] or ''}\n")

    n_fraud = sum(1 for p in players if p["fraud_type"])
    print(f"{len(players)} players ({n_fraud} fraud) -> {len(all_events)} events")
    print(f"wrote {events_path}")
    print(f"wrote {ledger_path}")


if __name__ == "__main__":
    main()