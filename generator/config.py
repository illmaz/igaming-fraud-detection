from dataclasses import dataclass

@dataclass
class GenConfig:
    seed: int = 42
    n_players: int = 300
    days_span: int = 7                 # simulated activity window
    fraud_pct: float = 0.05            # split evenly between the two patterns
    high_roller_pct: float = 0.08      # NORMAL players who look risky (overlap noise)
    subtle_fraud_pct: float = 0.30     # fraction of suspicious_flow fraud that's understated
    bonus_abuse_cluster_size: tuple = (3, 6)  # accounts sharing one device_id
    inject_duplicates: bool = False
    inject_late_pct: float = 0.0       # fraction of events stamped late (event_ts << ingest order)
    out_dir: str = "data/raw"