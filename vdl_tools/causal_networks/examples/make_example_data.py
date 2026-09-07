"""
Generate a small synthetic causal-network survey so the analysis can be run in seconds.

Ten factors with a planted structure:
  * "Funding" and "Leadership buy-in" are upstream drivers that cause most other factors
  * "Staff capacity" is a hub: caused by the two drivers, causing several downstream factors
  * the rest are downstream outcomes
For every ordered pair of factors we simulate a handful of yes/no votes: mostly "yes" for planted
links, mostly "no" otherwise, with some noise. Writes:
  example_nodes.csv          Label, Theme
  example_links.csv          Source, Target (labels), yes, no      <- generic two-table input
  example_undercurrent.json  the same votes in raw Undercurrent export format
  example_links_unweighted.csv   just the planted links (Source, Target), for analyze_single_network

Open in PyCharm and hit Run to regenerate (the outputs are committed, so this is optional).
"""

import json
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
rng = np.random.default_rng(0)

factors = {  # label -> theme
    "Funding": "Resources",
    "Leadership buy-in": "Governance",
    "Staff capacity": "Resources",
    "Clear policies": "Governance",
    "Training programs": "People",
    "Community trust": "People",
    "Data sharing": "Systems",
    "Service quality": "Outcomes",
    "Public awareness": "Outcomes",
    "Volunteer retention": "People",
}
planted_links = [
    ("Funding", "Staff capacity"), ("Funding", "Training programs"), ("Funding", "Data sharing"),
    ("Leadership buy-in", "Staff capacity"), ("Leadership buy-in", "Clear policies"),
    ("Leadership buy-in", "Data sharing"),
    ("Staff capacity", "Training programs"), ("Staff capacity", "Service quality"),
    ("Staff capacity", "Volunteer retention"),
    ("Clear policies", "Data sharing"), ("Clear policies", "Service quality"),
    ("Training programs", "Service quality"), ("Training programs", "Volunteer retention"),
    ("Data sharing", "Service quality"),
    ("Service quality", "Community trust"), ("Service quality", "Public awareness"),
    ("Community trust", "Volunteer retention"), ("Public awareness", "Community trust"),
]

labels = list(factors)
nodes = pd.DataFrame({"Label": labels, "Theme": [factors[l] for l in labels]})

rows = []
for source in labels:
    for target in labels:
        if source == target:
            continue
        p_yes = 0.85 if (source, target) in planted_links else 0.15
        n_votes = rng.integers(3, 7)               # 3..6 real votes per pair
        yes = int(rng.binomial(n_votes, p_yes))
        rows.append(dict(Source=source, Target=target, yes=yes, no=int(n_votes - yes),
                         skips=int(rng.integers(0, 3))))
links = pd.DataFrame(rows)

nodes.to_csv(HERE / "example_nodes.csv", index=False)
links[["Source", "Target", "yes", "no"]].to_csv(HERE / "example_links.csv", index=False)
pd.DataFrame(planted_links, columns=["Source", "Target"]).to_csv(HERE / "example_links_unweighted.csv", index=False)

# the same data in raw Undercurrent format: uuids, and `votes` that also counts skips
uuids = {label: str(uuid.UUID(int=rng.integers(0, 2**63))) for label in labels}
export = {
    "elements": [{"id": uuids[l], "label": l} for l in labels],
    "connections": [{"id": str(uuid.UUID(int=rng.integers(0, 2**63))),
                     "from": uuids[r.Source], "to": uuids[r.Target],
                     "votes": int(r.yes + r.no + r.skips), "sum": int(r.yes - r.no),
                     "yes": int(r.yes), "no": int(r.no)}
                    for r in links.itertuples()],
}
with open(HERE / "example_undercurrent.json", "w") as f:
    json.dump(export, f)
print(f"Wrote example data for {len(nodes)} factors and {len(links)} voted pairs to {HERE}")
