"""Portfolio-vs-ecosystem engagement engine.

Shared machinery for VDL portfolio-comparison engagements: intake profiling,
entity resolution against a pinned ecosystem baseline, and the ID Mapping File
that anchors every downstream join.

Spec (living document): vdl-project-template/docs/specs/phase1-intake-entity-resolution.md
Engagement repos are instantiated from vdl-project-template and call into this
package via thin skills/scripts; all logic lives here so fixes reach every
engagement with a pull.
"""
