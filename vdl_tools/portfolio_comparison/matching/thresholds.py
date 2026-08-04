"""Auto-accept rules in one place, tunable from decisions-log data over time.

Entity resolution never fully auto-passes: only the two highest-precision
signals (exact domain, near-exact single-candidate name) skip review.
"""

# Exact registrable-domain match → auto-accept.
DOMAIN_EXACT_AUTO = True
DOMAIN_EXACT_CONFIDENCE = 0.99

# A single candidate at or above this name similarity → auto-accept.
NAME_SIM_AUTO = 0.95
# Candidates below this are not worth showing a reviewer.
NAME_SIM_CANDIDATE_FLOOR = 0.70

MAX_CANDIDATES = 5
