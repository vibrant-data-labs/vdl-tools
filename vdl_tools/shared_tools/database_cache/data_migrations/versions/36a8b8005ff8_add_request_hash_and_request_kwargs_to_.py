"""add request_hash and request_kwargs to prompt_response

Adds kwargs-aware cache keys (CACHE_REFACTOR_PLAN.md Step 4):

- `request_hash`: hash of the allowlisted output-affecting API kwargs
  (reasoning, tools, temperature, ...). Existing rows backfill to '' via the
  server default, which is also what calls with no allowlisted kwargs hash
  to — so legacy rows keep matching those calls.
- `request_kwargs`: the normalized kwargs the hash was computed from, kept
  for auditability.
- PK widens from (prompt_id, given_id, model_name) to include request_hash,
  so calls differing only in output-affecting kwargs occupy separate rows.

Revision ID: 36a8b8005ff8
Revises: 8fa633af5bbd
Create Date: 2026-07-16 12:20:49.752595

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '36a8b8005ff8'
down_revision: Union[str, None] = '8fa633af5bbd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # server_default='' backfills all existing rows in one pass, and keeps
    # inserts from deployments that predate the ORM change valid.
    op.add_column('prompt_response', sa.Column('request_hash', sa.String(), server_default='', nullable=False))
    op.add_column('prompt_response', sa.Column('request_kwargs', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.create_index(op.f('ix_prompt_response_request_hash'), 'prompt_response', ['request_hash'], unique=False)

    # Widen the PK to include request_hash (same pattern as 223e827c20cd,
    # which widened it to include model_name).
    op.drop_constraint('prompt_response_pkey', 'prompt_response', type_='primary')
    op.create_primary_key(
        'prompt_response_pkey',
        'prompt_response',
        ['prompt_id', 'given_id', 'model_name', 'request_hash'],
    )


def downgrade() -> None:
    op.drop_constraint('prompt_response_pkey', 'prompt_response', type_='primary')

    # Rows that differ only in request_hash collide under the narrower PK —
    # keep the most recently updated one per (prompt_id, given_id, model_name).
    op.execute("""
        DELETE FROM prompt_response pr
        USING (
            SELECT prompt_id, given_id, model_name, request_hash,
                   ROW_NUMBER() OVER (
                       PARTITION BY prompt_id, given_id, model_name
                       ORDER BY date_updated DESC NULLS LAST,
                                date_added DESC NULLS LAST
                   ) AS rn
            FROM prompt_response
        ) ranked
        WHERE pr.prompt_id = ranked.prompt_id
          AND pr.given_id = ranked.given_id
          AND pr.model_name = ranked.model_name
          AND pr.request_hash = ranked.request_hash
          AND ranked.rn > 1
    """)

    op.drop_index(op.f('ix_prompt_response_request_hash'), table_name='prompt_response')
    op.drop_column('prompt_response', 'request_kwargs')
    op.drop_column('prompt_response', 'request_hash')
    op.create_primary_key(
        'prompt_response_pkey',
        'prompt_response',
        ['prompt_id', 'given_id', 'model_name'],
    )
