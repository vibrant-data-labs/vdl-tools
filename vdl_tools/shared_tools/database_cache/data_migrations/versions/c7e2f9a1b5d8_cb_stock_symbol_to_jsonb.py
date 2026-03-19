"""cb_organizations: change stock_symbol from varchar to jsonb

Revision ID: c7e2f9a1b5d8
Revises: b4f8a1c2d3e4
Create Date: 2026-03-19
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "c7e2f9a1b5d8"
down_revision: Union[str, None] = "b4f8a1c2d3e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "cb_organizations",
        "stock_symbol",
        type_=JSONB,
        postgresql_using="stock_symbol::jsonb",
    )


def downgrade() -> None:
    op.alter_column(
        "cb_organizations",
        "stock_symbol",
        type_=sa.String(),
        postgresql_using="stock_symbol::text",
    )
