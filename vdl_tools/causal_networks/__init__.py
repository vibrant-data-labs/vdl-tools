"""
Keystone Factor Analysis (KFA): structural analysis of voted causal networks.
Also called catalytic factor analysis.

A causal network here is a set of factors (nodes) and directed "if X improves, Y improves too"
links between them, typically gathered by voting (e.g. Kumu's Undercurrent survey).
The package ranks factors by two structural ideas borrowed from ecology:

* keystone leverage  - few incoming controls, many direct and 2-hop outgoing influences
* upstream position  - low "trophic level": a root cause rather than a downstream symptom

and combines them into a catalytic score, with an ensemble of vote thresholds and monte-carlo
link deletion to account for uncertainty in the votes.

Modules (import them explicitly, e.g. `from vdl_tools.causal_networks import pipeline`):

    load_data  - get any nodes/links source into the standard two tables
    metrics    - node metrics (2-hop degrees, keystone index, trophic level), clusters, layouts
    ensemble   - one network per vote threshold, ensemble aggregation, monte-carlo thinning
    pipeline   - one function that runs the whole analysis and writes an Excel file
    plots      - keystone vs upstream scatter plot (Altair)
    player     - openmappr network player (py2mappr)

See README.md in this folder for the method, the data requirements, and a quickstart.
"""
