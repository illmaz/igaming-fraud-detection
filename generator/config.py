"""
Tunable knobs for the BetStream synthetic generator.

Everything the generator's shape depends on lives here as a named constant with
a short note on WHY the value is what it is. The generated stream is the fixture
the fraud models are scored against in M7, so the *shape* of the data matters as
much as the volume: realism has to be dense enough to be a real test, without
washing out the two planted fraud patterns.
"""
from dataclasses import dataclass


@dataclass
class GenConfig:
    # ------------------------------------------------------------------ basics
    seed: int = 42
    n_players: int = 300
    days_span: int = 7                 # simulated activity window
    out_dir: str = "data/raw"

    # -------------------------------------------------------------- population
    fraud_pct: float = 0.05            # split evenly between the two patterns
    high_roller_pct: float = 0.08      # NORMAL players who look risky (overlap noise)
    subtle_fraud_pct: float = 0.30     # fraction of suspicious_flow fraud that's understated
    bonus_abuse_cluster_size: tuple = (3, 6)  # accounts sharing one device_id

    # ---------------------------------------------------------- session cadence
    # Real players arrive in bursts, not on a uniform clock. Session counts follow
    # a power law: most players show up once or twice, a small tail is very active.
    max_sessions: int = 15
    session_pareto_alpha: float = 1.6  # lower alpha => fatter tail of very active players
    min_session_gap_hours: float = 3.0 # sessions closer than this collapse into one
    churn_pct: float = 0.50            # half the players are only ever seen early...
    churn_window_frac: float = 0.25    # ...confined to the first quarter of the span
    bets_per_session: tuple = (3, 40)
    session_minutes: tuple = (15, 120)
    bet_gap_seconds: tuple = (20, 300) # clamp band for the derived inter-bet gap

    # Diurnal shape: relative session-start weight per hour. Evening peak 18:00-01:00,
    # dead 03:00-07:00. Without this, hourly aggregates downstream are white noise.
    hour_weights: tuple = (
        0.90, 0.55, 0.30, 0.15, 0.10, 0.10, 0.15, 0.25,   # 00-07
        0.35, 0.40, 0.45, 0.50, 0.60, 0.60, 0.60, 0.65,   # 08-15
        0.70, 0.85, 1.40, 1.80, 2.00, 2.00, 1.80, 1.40,   # 16-23
    )
    weekend_multiplier: float = 1.6    # Sat/Sun session volume vs a weekday

    # ----------------------------------------------------------------- amounts
    # Deposits cluster hard on round numbers because that is what the cashier UI
    # offers as one-tap buttons; the rest are odd card top-ups.
    deposit_round_amounts: tuple = (10, 20, 50, 100, 200, 500)
    deposit_round_weights: tuple = (18, 26, 24, 18, 9, 5)   # small deposits dominate
    deposit_round_pct: float = 0.85
    deposit_odd_range: tuple = (15.0, 300.0)

    # Stakes are log-normal: a mass of small bets with an occasional large one.
    bet_lognorm_mu: float = 1.8        # exp(1.8) ~= 6.0 => median stake ~6
    bet_lognorm_sigma: float = 0.9     # p95 ~= 26, long right tail
    bet_min: float = 1.0
    bet_max_frac_of_balance: float = 0.5  # nobody shoves the whole balance on one bet

    # Payouts. There is no bet_won event type, so a win only moves the balance.
    # win_prob * mean(win_multiplier) + big_win_prob * mean(big_win_multiplier)
    # lands RTP ~= 0.95, which is what keeps balances from exploding or dying instantly.
    win_prob: float = 0.32
    win_multiplier: tuple = (1.3, 2.6)
    big_win_prob: float = 0.015
    big_win_multiplier: tuple = (10.0, 35.0)
    big_win_threshold: float = 5.0     # payout/stake above this makes a player plan a cash-out

    # ------------------------------------------------------------- withdrawals
    withdrawal_delay_days: tuple = (1.0, 5.0)  # "won Saturday, cashed out midweek"
    withdrawal_frac: tuple = (0.4, 1.0)        # of the balance held at that moment
    withdrawal_min: float = 10.0
    idle_withdrawal_prob: float = 0.35 # cash-outs with no big win behind them

    # ----------------------------------------------------------- profile shapes
    # High roller: big money AND high turnover. Legit, but shaped like fraud.
    hr_deposit: tuple = (2000.0, 8000.0)
    hr_bets_per_session: tuple = (15, 60)
    hr_bet_frac_of_deposit: tuple = (0.02, 0.12)   # => 0.3x-7x turnover on the deposit
    hr_fast_cashout_pct: float = 0.25  # a quarter of HR visits cash out fast: the grey zone

    # suspicious_flow: deposit big, wager almost nothing, cash out fast.
    sf_deposit: tuple = (1500.0, 6000.0)
    sf_cycles: tuple = (1, 2)
    sf_bets: tuple = (2, 6)                        # a handful of token bets, not a single one
    sf_wager_frac: tuple = (0.01, 0.08)            # blatant: next to nothing wagered
    sf_wager_frac_subtle: tuple = (0.15, 0.35)     # understated: overlaps a fast-cashing HR
    sf_cashout_minutes: tuple = (5.0, 20.0)
    sf_cashout_minutes_subtle: tuple = (30.0, 90.0)
    sf_withdraw_frac: tuple = (0.85, 1.0)          # of the deposit, capped by balance

    # bonus_abuse: behaviourally unremarkable. The only tell is the shared device_id,
    # so these accounts deposit the minimum and don't stick around.
    ba_deposit_amounts: tuple = (10, 20, 50)
    ba_max_sessions: int = 4
    ba_max_bets_per_session: int = 12

    # ---------------------------------------------------------------- lateness
    # Lateness is DELIVERY LAG, not "it happened earlier": event_ts moves back by the
    # lag while the row keeps its file position, so it arrives behind events that
    # happened after it. Sub-minute lag is the common case (mobile client buffering,
    # one producer retry); the minutes-long tail is a client that went offline and
    # flushed its buffer on reconnect.
    inject_late_pct: float = 0.02
    late_short_pct: float = 0.97
    late_short_seconds: tuple = (2.0, 90.0)
    late_long_seconds: tuple = (120.0, 900.0)

    # -------------------------------------------------------------- duplicates
    # Producer retry after an ack timeout: the identical payload is re-sent a few
    # seconds later, so the copy lands a few rows down the file rather than next
    # to its original.
    inject_duplicates: bool = True
    duplicate_pct: float = 0.01
    duplicate_lag_seconds: tuple = (1.0, 30.0)
