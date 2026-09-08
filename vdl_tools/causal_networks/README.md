# Keystone Factor Analysis

Which parts of a big, messy problem, if solved, would have the biggest cascading effect on the rest?

**Keystone Factor Analysis (KFA)**, also called catalytic factor analysis, answers that question
structurally; this package (`vdl_tools.causal_networks`) implements it. Break a problem into its critical factors, collect
"if X improves, does Y improve too?" judgements between them, and treat the result as a directed
network. Two ideas borrowed from ecology then rank the factors:

* **Keystone leverage** - like a keystone species, a factor that few things control but that
  influences many others, directly and one step removed.
* **Upstream position** - like trophic level in a food web, where a factor sits in the flow of
  causation: a root cause (upstream) or a downstream symptom.

Factors that are both high-leverage and upstream are the **keystone factors**; they get the highest
**Catalytic Score**. Because the
link data come from votes, everything is computed on an ensemble of networks and repeated with random
link deletion, so every score is a mean across many plausible networks, with a standard deviation.

This is not a system-dynamics model and does not simulate anything. It maps every known, meaningfully
strong direct causal link and reads the structure of that map.

---

## Minimum data requirements

The analysis runs on two pandas tables. Nothing else is required.

| table | required | optional |
|---|---|---|
| `nodes` (one row per factor) | `Label` - the factor text (unique) | any other column (a theme, a short name, ...) is carried through to the output |
| `links` (one row per directed link, source -> target) | `Source`, `Target` - node labels (or ids, see loaders) | `yes`, `no` vote counts **or** `weight` **or** nothing; any other column is carried through for display |

Example `nodes.csv`:

| Label | Theme |
|---|---|
| Funding | Resources |
| Staff capacity | Resources |
| Service quality | Outcomes |

Example `links.csv` (voted):

| Source | Target | yes | no |
|---|---|---|---|
| Funding | Staff capacity | 4 | 0 |
| Funding | Service quality | 1 | 3 |
| Staff capacity | Service quality | 5 | 1 |

Which analysis applies depends only on the optional link columns:

| links have | analysis | function |
|---|---|---|
| `yes`, `no` | one network per vote threshold -> ensemble -> monte-carlo link deletion | `pipeline.run_causal_network_analysis` |
| `weight` | one network (threshold the weight yourself with `load_data.threshold_weights`), optional monte-carlo | `pipeline.analyze_single_network` |
| nothing | one network as given, optional monte-carlo | `pipeline.analyze_single_network` |

### Loaders (all in `load_data.py`)

| function | input | notes |
|---|---|---|
| `from_tables(nodes_df, links_df)` | any two DataFrames | the generic entry point; column names configurable; endpoints may be labels or the values of a node id column |
| `from_undercurrent(path)` | raw export from [Undercurrent](https://kumu.io), the pairwise-voting survey tool developed by Kumu; `.json` or two-sheet `.xlsx` | recomputes `votes = yes + no`; drops the raw `votes` (it counts skips) and `sum` |
| `from_kumu_weighted(path)` | Kumu Elements/Connections `.xlsx` with a `weight` column | untested against real data |

All loaders return `nodes` with an integer `id` (0..N-1, sorted by label) and `links` with integer
`Source`/`Target` plus readable `fromName`/`toName`. **Skips are not votes**: for voted links,
`votes = yes + no` and `fracYes = yes / votes`.

### What a link means

A link Source -> Target means "if Source improves, Target improves too" (the question Kumu's
Undercurrent survey asks). Links are **unsigned** and every metric below ignores sign. If your data carry a sign, a
confidence, or any other link attribute, keep the column: the loaders, the ensemble aggregation
(numeric columns averaged, text columns keep their first value) and the Excel/player outputs all carry
it through for display and filtering.

**Planned: signed links.** Extending the metrics to distinguish "X improves Y" from "X worsens Y"
(e.g. separate positive and negative 2-hop reach) is a planned next step. The pre-2024 signed-vote
code in the `manitoba-kfa` repo history (formerly `kumu`, `process_kumu_data.py`) is the reference for
the signed link format.

---

## The ecological framing

**Keystones.** On rocky shores, starfish eat mussels; unchecked, mussels crowd out everything else.
By eating mussels the starfish indirectly keeps dozens of species present, and nothing much eats the
starfish. Few incoming controls, many outgoing influences within two hops: that asymmetry is what we
look for in a factor.

**Trophic level.** Algae are eaten by zooplankton, which are eaten by fish. Trophic level = 1 + the
mean trophic level of what you eat, which sorts a whole food web into a loose hierarchy of energy flow.
Replace "eats" with "is caused by" and the same calculation sorts factors along the flow of causation:
root causes at the bottom (low level, upstream), symptoms at the top (high level, downstream). A
theory of change is one chain; a real problem has thousands, and this handles all of them at once.

---

## Metrics (per node, per network)

With N nodes and `outout` = number of nodes reachable in exactly 2 outgoing hops,
`inin` = number of nodes that reach this node in exactly 2 incoming hops:

| column | formula | range |
|---|---|---|
| `2_Degree_Asymmetry` | (outout - inin) / (outout + inin) | -1 .. 1 |
| `2_Degree_Reach` | 100 * outout / N | 0 .. 100 (% of all nodes) |
| `Keystone_Index` | minmax(reach) * minmax(asymmetry) | 0 .. 1 within the network |
| `Keystone_Pctl` | percentile rank of Keystone_Index | 0 .. 100 |
| `Trophic_Level` | minmax(rooted trophic level) | 0 = most upstream .. 1 = most downstream |
| `Upstream_Score` | 1 - Trophic_Level | 1 = most upstream |
| `Upstream_Pctl` | percentile rank of Upstream_Score | 0 .. 100, high = more upstream |
| `Catalytic Score` | Keystone Rank * Upstream Score (rounded) | 0 .. 100 |

plus `total_links`, `in_degree`, `out_degree`, `betweenness`, `1_Degree_Asymmetry`, directed Louvain
`Cluster` with `ClusterBridging` / `ClusterCentrality`.

Rooted trophic level: reverse the graph so arrows point from effect to cause, attach a synthetic root
node beneath every node (so feedback loops do not break the linear system), solve
`level = 1 + mean(level of causes)`, drop the root, shift so the minimum is 1. Root causes and isolated
nodes share the minimum.

Percentile ranks use `rank(method="max")`, scaled to 0..100. A percentile of 0 or 100 means the lowest
or highest value in the network, ties share the top of their group.

---

## Dealing with uncertain votes

1. **One network per vote threshold.** A link needs at least `min_votes` (default 3) votes to count at
   all. Then for every "% yes" cutoff from `min_pct` (20) to `max_pct` (80) we keep the links with at
   least that share of yes votes, build the network, and score every node. Networks that are too dense
   (connectance > `max_connectance`, default 0.4) or too fragmented (more than `max_isolated` isolated
   nodes, default 1) are dropped.
2. **Ensemble network.** Node metrics are averaged across the kept networks. A link is in the ensemble
   if it is present in at least `link_frac_thresh` (0.5) of them.
3. **Monte-carlo link deletion.** On the ensemble network, delete `del_frac` (0.15) of the links at
   random, rescore every node, repeat `n_trials` (100) times. Reported values are the mean across
   trials; `Keystone Rank (Std Dev)` and `Upstream Rank (Std Dev)` are the spread.

Two things to know about this procedure:

* Ranks are the **mean of the per-trial ranks**, not the rank of the mean score. This is deliberate:
  it rewards factors that rank highly across many plausible networks.
* Every trial graph contains every node, so a node that happens to lose all its links in a trial is
  scored as isolated (reach 0, asymmetry 0) rather than skipped. `n_trials` is the same for every node.

Reproducibility: pass `seed`. The monte-carlo is exactly reproducible; the ensemble link set is
deterministic regardless; layout coordinates are reproducible in practice (the layout uses numpy's
global random state, which is seeded before each layout call).

---

## Parameters of `run_causal_network_analysis`

| parameter | default | meaning |
|---|---|---|
| `min_votes` | 3 | minimum votes for a link to be eligible |
| `min_pct`, `max_pct`, `pct_step` | 20, 80, 1 | range of "% yes" thresholds |
| `max_connectance` | 0.4 | drop threshold networks denser than this |
| `max_isolated` | 1 | drop threshold networks with more isolated nodes |
| `link_frac_thresh` | 0.5 | ensemble keeps links present in this fraction of networks |
| `n_trials`, `del_frac` | 100, 0.15 | monte-carlo trials and fraction of links deleted per trial |
| `seed` | 42 | random seed |
| `top_keystone_pctl` | 80 | Keystone Rank at or above this is flagged `Top Keystone` |
| `group_attr` | "Pillar" | categorical node column for the layout and player colors (falls back to `Causal Cluster`) |
| `node_attributes`, `node_attribute_cols` | None | hand-curated node columns to merge on `Label` (e.g. a short name and a theme) |
| `cluster_layout`, `group_layout` | see `pipeline.py` | tag2network layout settings |

---

## Output columns (Excel sheet `Nodes`, and the player)

| column | meaning | old name (pre-2026 deliverables) |
|---|---|---|
| Root Factor, Name | full label, curated short name | same |
| Catalytic Score | Keystone Rank x Upstream Score | same number; was Keystone Rank x (1 - Upstream Position) |
| Top Keystone | Yes / No | "True " / "False " |
| Keystone Rank | mean percentile of Keystone Score | Keystone Rank (Avg) |
| Keystone Score | mean Keystone Index | Keystone Score (Avg) |
| Upstream Rank | mean percentile of Upstream Score; **high = upstream** | Upstream Rank (Avg), where **low** = upstream |
| Upstream Score | 1 - Trophic Level | (new) |
| Trophic Level | 0 = upstream, 1 = downstream | Upstream Position (Avg) |
| Reach in 2 Hops (Pct) | percent of nodes reached in 2 hops | same name, but was a 0-1 fraction |
| Degree, Incoming Links, Outgoing Links, Betweenness | 1-hop counts | Degree (Avg), Incoming (Avg), Outgoing (Avg) |
| Causal Cluster, Cluster Bridging, Cluster Centrality | directed Louvain community and cluster metrics | same |
| Keystone Rank (Std Dev), Upstream Rank (Std Dev), n_trials | monte-carlo spread and trial count | dropped |
| label, x, y, id | used by the player | same |

Any extra node column you supplied (e.g. `Theme`) appears after the standard columns. The `Links` sheet
has `Source`, `Target`, `fromName`, `toName`, the vote columns, `n_networks_with_link`,
`frac_networks`, and any extra link columns.

---

## Quickstart

```python
import pandas as pd
from vdl_tools.causal_networks import load_data, pipeline, plots, player

nodes, links = load_data.from_tables(pd.read_csv("nodes.csv"), pd.read_csv("links.csv"))
nodes, links = pipeline.run_causal_network_analysis(nodes, links, out_xlsx="results.xlsx",
                                                    n_trials=100, seed=42, group_attr="Theme")
plots.keystone_vs_upstream_scatter(nodes, out_html="scatter.html", color="Theme")
player.build_player(nodes, links, "player", title="My causal network", group_attr="Theme")
player.serve_player("player")   # opens http://localhost:8000 in your browser; Ctrl-C to stop
```

Which attributes the player shows where (filters, profile, search, axis/color/size dropdowns, hover
text) comes from a settings table with one row per attribute, the same `player_attribute_settings.xlsx`
format used across VDL projects. `player.write_attribute_settings("settings.xlsx")` writes the defaults;
edit in Excel and pass the path as `attribute_settings=`. `examples/player_attribute_settings.xlsx` is
that default sheet.

`examples/run_example.py` does exactly this on a 10-factor synthetic survey (open it in PyCharm and
hit Run; ~10 seconds). `examples/run_example_unweighted.py` shows the single-network path on a plain
link list. Regenerate the example data with `examples/make_example_data.py`.

For a real export from Kumu's Undercurrent survey replace the first line with
`nodes, links = load_data.from_undercurrent("survey-export.json")`. For a worked example on real data
see the `manitoba-kfa` repository (Manitoba / RRC Polytech, 73 factors).

## Module map

| module | what it holds |
|---|---|
| `load_data.py` | `from_tables`, `from_undercurrent`, `from_kumu_weighted`, `threshold_links`, `threshold_weights`, `build_graph`, `add_node_attributes` |
| `metrics.py` | `add_node_metrics`, `rooted_trophic_level`, `two_hop_out_degree`, `two_hop_in_degree`, `percentile_rank`, `add_clusters`, `add_group_layout`, `node_pair_similarities` |
| `ensemble.py` | `build_threshold_networks`, `aggregate_ensemble`, `monte_carlo_thinning` |
| `pipeline.py` | `run_causal_network_analysis`, `analyze_single_network`, `finalize_for_display`, `write_network_excel`, `DISPLAY_NAMES` |
| `plots.py` | `keystone_vs_upstream_scatter` |
| `player.py` | `build_player`, `DEFAULT_ATTRIBUTE_SETTINGS` (spreadsheet-style table: one row per attribute, 0/1 columns per setting, `Display_Name`, `Keep`, `tooltip`), `write_attribute_settings`, `read_attribute_settings`, `serve_player` (serve a built player at localhost:8000 and open the browser; blocks until Ctrl-C) |
| `tests/` | `pytest vdl_tools/causal_networks/tests` |

## Acknowledgements

Undercurrent, the pairwise causal-voting survey whose exports this package reads, is developed by
[Kumu](https://kumu.io). The keystone and trophic-level ideas come from ecology (Paine, 1966;
Martinez, 1991) and were adapted to problem structure by Vibrant Data Labs.

## Gotchas

* The raw Undercurrent `votes` column counts everyone shown the pair, including skips. Never use it
  directly; the loader recomputes `votes = yes + no`.
* `Trophic Level` is LOW for upstream factors. Use `Upstream Score` / `Upstream Rank` when you want
  "higher = more upstream".
* Hand-curated attribute files must match `Label` exactly; whitespace is stripped on both sides, and
  unmatched labels are reported with a warning. A node with no `group_attr` value drops the whole
  layout back to `Causal Cluster`.
* tag2network keys nodes by the DataFrame index, this package by the `id` column. `metrics.add_clusters`
  and `metrics.add_group_layout` align them; if you call tag2network directly, set the index to `id` first.
* Connectance is measured against all N*(N-1) ordered pairs, even if the survey did not ask about
  every pair.
* Small networks (under ~15 nodes) produce coarse percentiles and many ties; read ranks accordingly.
* `Causal Cluster` comes from tag2network's directed Louvain and depends on that library's version;
  cluster ids and counts are not comparable across runs made with different vdl-tools versions.
  Rankings (Keystone Rank, Upstream Rank, Catalytic Score) are the stable outputs.
