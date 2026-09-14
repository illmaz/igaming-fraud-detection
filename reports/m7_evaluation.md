# M7: Evaluation

Scoring both fraud rules against the ground truth ledger. The pipeline never
read this file at any stage of processing. M7 is the first time it is used.

## Method

Detections are collapsed from window level to player level before scoring. One
player tripping the rule across 40 windows is one case, not 40. Scoring at
window level would make recall exceed 1.0, since the ledger only has one row
per player.

A player counts as flagged if the rule fired for them in any window.

## Results

| rule | tp | fp | fn | precision | recall |
|------|----|----|----|-----------|--------|
| wager_ratio_alone | 7 | 181 | 0 | 0.04 | 1.00 |
| withdrawal_ratio_alone | 7 | 6 | 0 | 0.54 | 1.00 |
| m5_combined | 7 | 4 | 0 | 0.64 | 1.00 |
| m6_bonus_abuse | 5 | 0 | 3 | 1.00 | 0.62 |
| pipeline_combined | 12 | 4 | 3 | 0.75 | 0.80 |

Written to `data/delta/evaluation_metrics`.

## M5: suspicious flow

Caught all 7 real cases, but flagged 4 innocent players alongside them. More
than a third of the alert queue is noise.

The ablation is the more useful result. Wager ratio alone flags 181 false
positives and is close to worthless by itself. Withdrawal ratio alone does
better at 6. Only the two conditions together bring false positives down to 4.

Two mediocre signals combined beat the stronger single signal. That is the
justification for the rule using both conditions rather than whichever one
scored best on its own.

### Where the numbers are flattered

Withdrawal ratio looks like a strong separator because withdrawals are rare in
this dataset. Only 141 withdrawal events exist across 9,787 rows, and most
normal players never withdraw at all, because the house edge drains their
balance first. A feature that scores well on an axis where most of the
population has no data is not a feature that has been tested.

## M6: bonus abuse

Perfect precision, zero false positives. That number is flattered too, and
harder to defend than M5's.

The generator injected no legitimate device sharing anywhere in the data. There
are no shared households, no internet cafes, no resold handsets. So there is
nothing innocent for the rule to trip over, and precision of 1.00 measures the
absence of hard cases rather than the quality of the rule.

Recall of 0.62 is by design. The threshold is 5 accounts per device, and one
bonus abuse cluster sits at 3 accounts. It is missed. This was predicted in
writing before the evaluation ran, not discovered afterward.

## Pipeline combined

The two rules flag entirely non-overlapping sets of players. Combined true
positives are exactly the sum of each rule's, 7 plus 5. Every false positive
comes from M5. Every false negative comes from M6's threshold.

The non-overlap is expected. Bonus abuse players behave normally on deposits,
wagering and withdrawals, so no behavioural rule reaches them. The only signal
is the shared device. This is why M6 exists as a separate approach rather than
another condition bolted onto M5.

## What this does not prove

- Thresholds were chosen before looking at labels, but the data was generated
  by the same person writing the rules. Difficulty is self set.
- No legitimate device sharing exists in the data, so M6 precision does not
  generalise.
- M5 flags at window level, which is not the same as producing cases. Entity
  level aggregation with a scoring model belongs downstream, and does not
  exist here.
- 300 players is small enough that single players move the numbers visibly.