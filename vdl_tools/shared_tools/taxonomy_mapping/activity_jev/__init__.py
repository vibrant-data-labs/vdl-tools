"""Generic JEV subject mapping: engine, taxonomy structure, tracker-shaped exports and a runner.

``runs.run_activity_mapping`` is the entry point for a project: it extracts website
activities once and maps them onto each supplied taxonomy job.

Taxonomy-specific loaders, question wording and default policies live in sibling
packages (``taxonomy_mapping.one_earth``, ``taxonomy_mapping.drawdown``); nothing here
imports them or switches on a taxonomy name.
"""
