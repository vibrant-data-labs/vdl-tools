"""Map text onto taxonomies with Jev, and classify organizations from their website activities.

- ``mapping`` is generic: ``Taxonomy``, ``load_workbook``, ``MatchPolicy``,
  ``score_subjects``, ``select_matches`` and ``map_subjects`` take any text and return
  matched taxonomy paths.
- ``activities`` is specific to extracted activities and trackers: ``run_activity_mapping``
  (the entry point for a project), ``MappingJob``, ``build_activity_subjects``,
  ``export_extraction`` and ``assemble_mapping_runs``.

Taxonomy-specific loaders, question wording and defaults live in sibling packages
(``taxonomy_mapping.one_earth``, ``taxonomy_mapping.drawdown``); nothing here imports them
or switches on a taxonomy name.
"""
