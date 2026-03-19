"""cb_organizations: primary key on uuid only (fixes ON CONFLICT (uuid))

Revision ID: b4f8a1c2d3e4
Revises: 1a236e4007ba
Create Date: 2026-03-19

Earlier revisions used PRIMARY KEY (identifier, uuid) with JSONB identifier; the ORM
upserts on uuid only and PostgreSQL requires ON CONFLICT targets to match a unique/PK.
"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b4f8a1c2d3e4"
down_revision: Union[str, None] = "1a236e4007ba"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("cb_organizations_pkey", "cb_organizations", type_="primary")
    op.create_primary_key("cb_organizations_pkey", "cb_organizations", ["uuid"])


def downgrade() -> None:
    op.drop_constraint("cb_organizations_pkey", "cb_organizations", type_="primary")
    op.create_primary_key(
        "cb_organizations_pkey", "cb_organizations", ["identifier", "uuid"]
    )
