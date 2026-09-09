"""
Generates synthetic BetStream events + a ground-truth fraud ledger.

Output:
  data/raw/events.jsonl        - one JSON event per line, in ARRIVAL order
  data/raw/ground_truth.csv    - player_id, fraud_type (null | suspicious_flow | bonus_abuse)

The pipeline must NEVER read ground_truth.csv. It exists only for M7 evaluation.

Shape of the stream:
  * players play in SESSIONS - a login, a burst of bets minutes apart, maybe a
    deposit - not on a uniform clock; sessions cluster in the evening and on
    weekends, and about half the players churn after the first quarter of the span
  * stakes are log-normal, deposits cluster on round numbers, and a running
    balance is enforced: nobody bets or withdraws money they do not have
  * the two fraud patterns are layered ON TOP of that realism, not instead of it,
    and the high-roller grey zone is preserved on purpose
"""
import bisect
import json
import random
import statistics
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from config import GenConfig


# --------------------------------------------------------------------- players

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

    # normal but high-roller: big deposits, high turnover, sometimes a fast cashout,
    # but NOT fraud. This is your overlap noise against suspicious_flow - don't skip it.
    for _ in range(n_high_roller):
        players.append({
            "player_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "payment_method_id": str(uuid.uuid4()),
            "profile": "high_roller",
            "fraud_type": None,
        })

    # suspicious_flow fraud - some extreme, some subtle (subtle overlaps high_roller)
    for _ in range(n_suspicious):
        subtle = rng.random() < cfg.subtle_fraud_pct
        players.append({
            "player_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "payment_method_id": str(uuid.uuid4()),
            "profile": "suspicious_flow_subtle" if subtle else "suspicious_flow",
            "fraud_type": "suspicious_flow",
        })

    # bonus_abuse fraud - clustered by shared device_id
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


# -------------------------------------------------------------- session timing

def _n_sessions(cfg: GenConfig, rng: random.Random, cap: int = None):
    """Power-law session count: most players show up once or twice, a few a lot."""
    draw = rng.paretovariate(cfg.session_pareto_alpha)
    return int(min(cap or cfg.max_sessions, max(1, round(draw))))


def _session_starts(cfg: GenConfig, rng: random.Random, start_ts: datetime,
                    n_sessions: int, window_days: float):
    """Session start times, weighted to evenings and weekends, inside the player's window."""
    day_count = max(1, int(window_days))
    days = list(range(day_count))
    day_weights = [
        cfg.weekend_multiplier if (start_ts + timedelta(days=d)).weekday() >= 5 else 1.0
        for d in days
    ]
    hours = list(range(24))
    hour_weights = list(cfg.hour_weights)

    starts = []
    for _ in range(n_sessions):
        d = rng.choices(days, weights=day_weights, k=1)[0]
        h = rng.choices(hours, weights=hour_weights, k=1)[0]
        starts.append(start_ts + timedelta(days=d, hours=h, minutes=rng.uniform(0, 60)))
    starts.sort()

    # Two logins twenty minutes apart is one session, not two - collapse them.
    spaced = []
    for s in starts:
        if not spaced or (s - spaced[-1]).total_seconds() >= cfg.min_session_gap_hours * 3600:
            spaced.append(s)
    return spaced


# ------------------------------------------------------------- the player sim

class PlayerSim:
    """Per-player state: running balance, emitted events, and planned cash-outs."""

    def __init__(self, player, cfg: GenConfig, rng: random.Random, stats: dict):
        self.p = player
        self.cfg = cfg
        self.rng = rng
        self.stats = stats
        self.balance = 0.0
        self.events = []
        self.pending_withdrawals = []

    # -- raw emit -----------------------------------------------------------
    def emit(self, t: datetime, event_type: str, amount=None):
        self.events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "player_id": self.p["player_id"],
            "device_id": self.p["device_id"],
            "payment_method_id": self.p["payment_method_id"],
            "amount": None if amount is None else round(amount, 2),
            "event_ts": t.isoformat(),
        })

    # -- money --------------------------------------------------------------
    def deposit(self, t: datetime, amount: float = None, small: bool = False):
        cfg, rng = self.cfg, self.rng
        if amount is None:
            pool = cfg.ba_deposit_amounts if small else cfg.deposit_round_amounts
            weights = list(cfg.deposit_round_weights[:len(pool)])
            if rng.random() < cfg.deposit_round_pct:
                amount = float(rng.choices(list(pool), weights=weights, k=1)[0])
            else:
                amount = rng.uniform(*cfg.deposit_odd_range)
        amount = round(amount, 2)
        self.balance += amount
        self.emit(t, "deposit", amount)
        return amount

    def bet(self, t: datetime, stake: float = None):
        """Places a bet, settles it against the balance. Returns True if it happened."""
        cfg, rng = self.cfg, self.rng
        if stake is None:
            stake = rng.lognormvariate(cfg.bet_lognorm_mu, cfg.bet_lognorm_sigma)
            stake = min(stake, self.balance * cfg.bet_max_frac_of_balance)
        stake = min(stake, self.balance)
        if stake < cfg.bet_min:
            return False

        self.balance -= stake
        self.emit(t, "bet_placed", stake)

        r = rng.random()
        if r < cfg.big_win_prob:
            mult = rng.uniform(*cfg.big_win_multiplier)
        elif r < cfg.big_win_prob + cfg.win_prob:
            mult = rng.uniform(*cfg.win_multiplier)
        else:
            mult = 0.0
        self.balance += stake * mult

        # a proper win is what makes someone think about cashing out - days later
        if mult >= cfg.big_win_threshold:
            self.plan_withdrawal(t + timedelta(days=rng.uniform(*cfg.withdrawal_delay_days)))
        return True

    def withdraw(self, t: datetime, amount: float):
        amount = min(amount, self.balance)
        if amount < self.cfg.withdrawal_min:
            return 0.0
        self.balance -= amount
        self.emit(t, "withdrawal", amount)
        return amount

    # -- cash-out scheduling -------------------------------------------------
    def plan_withdrawal(self, when: datetime):
        self.pending_withdrawals.append(when)
        self.pending_withdrawals.sort()

    def flush_withdrawals(self, before: datetime):
        """Cash-outs come due in their OWN session, usually days after the win."""
        cfg, rng = self.cfg, self.rng
        while self.pending_withdrawals and self.pending_withdrawals[0] < before:
            when = self.pending_withdrawals.pop(0)
            if self.balance < cfg.withdrawal_min:
                continue
            self.emit(when - timedelta(seconds=rng.uniform(30, 240)), "login")
            self.withdraw(when, self.balance * rng.uniform(*cfg.withdrawal_frac))

    # -- stats ---------------------------------------------------------------
    def record_session(self, start: datetime, end: datetime, n_bets: int):
        if n_bets:
            self.stats["session_minutes"].append((end - start).total_seconds() / 60.0)
            self.stats["bets_per_session"].append(n_bets)


# ----------------------------------------------------------------- session kinds

def _play_session(sim: PlayerSim, t: datetime, small: bool = False):
    """An ordinary visit: login, top up if broke, a burst of bets minutes apart."""
    cfg, rng = sim.cfg, sim.rng
    start = t
    sim.emit(t, "login")

    n_bets = rng.randint(*cfg.bets_per_session)
    if small:
        n_bets = min(n_bets, cfg.ba_max_bets_per_session)
    duration = rng.uniform(*cfg.session_minutes)
    # gap follows from the session length and the bet count, clamped to the realistic band
    lo, hi = cfg.bet_gap_seconds
    mean_gap = max(lo, min(hi, duration * 60.0 / n_bets))

    # deposit at session start when the balance can't cover a typical stake
    if sim.balance < cfg.bet_min * 5:
        t += timedelta(seconds=rng.uniform(10, 120))
        sim.deposit(t, small=small)

    placed = 0
    for _ in range(n_bets):
        if sim.balance < cfg.bet_min:
            # ran dry mid-session: top up, or give up and leave
            if rng.random() < 0.6:
                t += timedelta(seconds=rng.uniform(20, 180))
                sim.deposit(t, small=small)
            else:
                break
        if sim.bet(t):
            placed += 1
        t += timedelta(seconds=max(5.0, rng.uniform(0.5, 1.5) * mean_gap))

    sim.record_session(start, t, placed)
    if sim.balance >= cfg.withdrawal_min and rng.random() < cfg.idle_withdrawal_prob:
        sim.plan_withdrawal(t + timedelta(days=rng.uniform(*cfg.withdrawal_delay_days)))
    return t


def _high_roller_session(sim: PlayerSim, t: datetime):
    """Big deposit, high turnover. Legit - but a quarter of visits cash out fast."""
    cfg, rng = sim.cfg, sim.rng
    start = t
    sim.emit(t, "login")

    deposit_amt = round(rng.uniform(*cfg.hr_deposit) / 50.0) * 50.0
    t += timedelta(seconds=rng.uniform(30, 300))
    sim.deposit(t, amount=deposit_amt)

    fast_cashout = rng.random() < cfg.hr_fast_cashout_pct
    n_bets = rng.randint(*cfg.hr_bets_per_session)
    if fast_cashout:
        # short visit, low turnover, quick withdrawal: deliberately shaped like
        # subtle suspicious_flow. This grey zone is the point.
        n_bets = max(3, n_bets // 6)

    placed = 0
    for _ in range(n_bets):
        stake = deposit_amt * rng.uniform(*cfg.hr_bet_frac_of_deposit)
        if not sim.bet(t, stake=stake):
            break
        placed += 1
        t += timedelta(seconds=rng.uniform(*cfg.bet_gap_seconds))

    sim.record_session(start, t, placed)
    if fast_cashout:
        t += timedelta(minutes=rng.uniform(15, 60))
        sim.withdraw(t, sim.balance * rng.uniform(0.7, 1.0))
    elif sim.balance >= cfg.withdrawal_min and rng.random() < cfg.idle_withdrawal_prob:
        sim.plan_withdrawal(t + timedelta(days=rng.uniform(*cfg.withdrawal_delay_days)))
    return t


def _suspicious_flow_session(sim: PlayerSim, t: datetime):
    """Deposit big, wager a token fraction of it, withdraw almost all of it, fast."""
    cfg, rng = sim.cfg, sim.rng
    subtle = sim.p["profile"].endswith("subtle")
    start = t
    sim.emit(t, "login")

    deposit_amt = round(rng.uniform(*cfg.sf_deposit) / 10.0) * 10.0
    t += timedelta(seconds=rng.uniform(30, 300))
    deposit_ts = t
    sim.deposit(t, amount=deposit_amt)

    wager_frac = rng.uniform(*(cfg.sf_wager_frac_subtle if subtle else cfg.sf_wager_frac))
    cashout_min = rng.uniform(*(cfg.sf_cashout_minutes_subtle if subtle else cfg.sf_cashout_minutes))

    # the token wagering, split over a handful of bets inside the cash-out window
    n_bets = rng.randint(*cfg.sf_bets)
    shares = [rng.uniform(0.5, 1.5) for _ in range(n_bets)]
    total_share = sum(shares)
    placed = 0
    for i, share in enumerate(shares, start=1):
        t = deposit_ts + timedelta(minutes=cashout_min * (i / (n_bets + 1.0)))
        if sim.bet(t, stake=deposit_amt * wager_frac * share / total_share):
            placed += 1

    sim.record_session(start, t, placed)

    # fast withdrawal of almost the whole deposit - the actual signal
    t = deposit_ts + timedelta(minutes=cashout_min)
    sim.withdraw(t, deposit_amt * rng.uniform(*cfg.sf_withdraw_frac))
    return t


# ------------------------------------------------------------ per-player driver

def gen_events_for_player(player, cfg: GenConfig, rng: random.Random,
                          start_ts: datetime, stats: dict):
    """Returns a list of event dicts for one player, timestamps within cfg.days_span."""
    sim = PlayerSim(player, cfg, rng, stats)
    profile = player["profile"]
    span_end = start_ts + timedelta(days=cfg.days_span)

    # most players churn: only ever seen in the first quarter of the span
    churned = rng.random() < cfg.churn_pct
    window_days = cfg.days_span * (cfg.churn_window_frac if churned else 1.0)

    if profile in ("suspicious_flow", "suspicious_flow_subtle"):
        n_sessions = rng.randint(*cfg.sf_cycles)
    elif profile == "bonus_abuse":
        n_sessions = _n_sessions(cfg, rng, cap=cfg.ba_max_sessions)
    else:
        n_sessions = _n_sessions(cfg, rng)

    starts = _session_starts(cfg, rng, start_ts, n_sessions, window_days)
    if not starts:
        return []

    # account creation shortly before the first login
    created = max(start_ts, starts[0] - timedelta(minutes=rng.uniform(5, 240)))
    sim.emit(created, "player_created")

    for s in starts:
        sim.flush_withdrawals(before=s)
        if profile in ("suspicious_flow", "suspicious_flow_subtle"):
            _suspicious_flow_session(sim, s)
        elif profile == "high_roller":
            _high_roller_session(sim, s)
        else:
            _play_session(sim, s, small=(profile == "bonus_abuse"))

    sim.flush_withdrawals(before=span_end)
    return [e for e in sim.events if e["event_ts"] <= span_end.isoformat()]


# ------------------------------------------------------------------- reporting

def _pct(values, q):
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[idx]


def main():
    cfg = GenConfig()
    rng = random.Random(cfg.seed)

    out_dir = Path(cfg.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    players = make_players(cfg, rng)
    # floored to the hour so a rerun with the same seed reproduces the same shape
    start_ts = (datetime.utcnow() - timedelta(days=cfg.days_span)).replace(
        minute=0, second=0, microsecond=0)

    stats = {"session_minutes": [], "bets_per_session": []}
    all_events = []
    for p in players:
        all_events.extend(gen_events_for_player(p, cfg, rng, start_ts, stats))

    # Clean chronological baseline. Everything below deliberately perturbs
    # arrival order relative to event_ts - producers/ replays this file in
    # strict line order, so file position IS arrival order.
    all_events.sort(key=lambda e: e["event_ts"])
    arrival = [datetime.fromisoformat(e["event_ts"]) for e in all_events]

    # Late events: DELIVERY LAG. event_ts moves back by the lag the payload spent
    # stuck in a client buffer, while the row keeps its file position - so it
    # arrives behind events that happened after it. Sub-minute is the common case.
    lags = []
    n_late = int(len(all_events) * cfg.inject_late_pct)
    if n_late:
        for e in rng.sample(all_events, k=n_late):
            if rng.random() < cfg.late_short_pct:
                lag = rng.uniform(*cfg.late_short_seconds)
            else:
                lag = rng.uniform(*cfg.late_long_seconds)
            original = datetime.fromisoformat(e["event_ts"])
            e["event_ts"] = (original - timedelta(seconds=lag)).isoformat()
            lags.append(lag)

    # Duplicates: producer retry after an ack timeout. The identical payload is
    # re-sent 1-30s later, so the copy lands wherever the stream had reached by
    # then - a few rows down, not adjacent to its original.
    n_dupes = 0
    if cfg.inject_duplicates:
        n_dupes = int(len(all_events) * cfg.duplicate_pct)
        pending = {}
        for i in rng.sample(range(len(all_events)), k=n_dupes):
            retry_at = arrival[i] + timedelta(seconds=rng.uniform(*cfg.duplicate_lag_seconds))
            j = min(max(bisect.bisect_right(arrival, retry_at) - 1, i), len(all_events) - 1)
            pending.setdefault(j, []).append(dict(all_events[i]))
        with_dupes = []
        for idx, e in enumerate(all_events):
            with_dupes.append(e)
            with_dupes.extend(pending.get(idx, ()))
        all_events = with_dupes

    # NOTE: do NOT re-sort all_events here. A re-sort would restore perfect
    # chronological order and silently undo both tests above - late events
    # would land exactly where their timestamp belongs (not late at all), and
    # duplicates would sit adjacent to their originals.

    events_path = out_dir / "events.jsonl"
    with open(events_path, "w") as f:
        for e in all_events:
            f.write(json.dumps(e) + "\n")

    ledger_path = out_dir / "ground_truth.csv"
    with open(ledger_path, "w") as f:
        f.write("player_id,fraud_type\n")
        for p in players:
            f.write(f"{p['player_id']},{p['fraud_type'] or ''}\n")

    # ------------------------------------------------------------------ report
    by_type = {}
    for e in all_events:
        by_type[e["event_type"]] = by_type.get(e["event_type"], 0) + 1
    by_fraud = {}
    for p in players:
        key = p["fraud_type"] or "none"
        by_fraud[key] = by_fraud.get(key, 0) + 1

    ts_values = [e["event_ts"] for e in all_events]
    inversions = sum(1 for i in range(1, len(ts_values)) if ts_values[i] < ts_values[i - 1])

    first_seen, dupe_gaps = {}, []
    for idx, e in enumerate(all_events):
        if e["event_id"] in first_seen:
            dupe_gaps.append(idx - first_seen[e["event_id"]])
        else:
            first_seen[e["event_id"]] = idx

    print(f"{len(players)} players -> {len(all_events)} events")
    print("  by event_type: " + ", ".join(
        f"{k}={by_type.get(k, 0)}" for k in
        ("player_created", "login", "deposit", "bet_placed", "withdrawal")))
    print("  by fraud_type: " + ", ".join(f"{k}={v}" for k, v in sorted(by_fraud.items())))
    print(f"  event span: {min(ts_values)} -> {max(ts_values)}")
    print(f"  median session: {statistics.median(stats['session_minutes']):.1f} min, "
          f"median bets/session: {statistics.median(stats['bets_per_session']):.0f}")
    print(f"injected {n_late} late events, {n_dupes} duplicates")
    if lags:
        print(f"  lateness seconds: min={min(lags):.1f} median={statistics.median(lags):.1f} "
              f"p95={_pct(lags, 0.95):.1f} max={max(lags):.1f}")
    if dupe_gaps:
        print(f"  duplicate pairs: {len(dupe_gaps)}, all copies after their original, "
              f"row gap min={min(dupe_gaps)} median={statistics.median(dupe_gaps):.0f} "
              f"max={max(dupe_gaps)}")
    print(f"  file NOT globally sorted: {inversions} out-of-order rows "
          f"({'OK' if inversions else 'FAIL - re-sort leaked in'})")
    print(f"wrote {events_path}")
    print(f"wrote {ledger_path}")


if __name__ == "__main__":
    main()
