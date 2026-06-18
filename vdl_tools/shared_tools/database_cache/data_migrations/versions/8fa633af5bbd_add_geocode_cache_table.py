"""add geocode cache table

Revision ID: 8fa633af5bbd
Revises: 5b984553ab92
Create Date: 2026-06-18 15:42:50.806397

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '8fa633af5bbd'
down_revision: Union[str, None] = '5b984553ab92'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # NOTE: autogenerate also proposed dropping `grantor_recipient_labeled_set`,
    # `microapp_configs`, and `microapp_domains`. Those tables exist in the DB but
    # are NOT SQLAlchemy models in this repo (owned by other services), so the
    # drops were removed by hand. This migration only creates the `geocode` table.
    op.create_table('geocode',
    sa.Column('provider', sa.String(), nullable=False),
    sa.Column('address_id', sa.String(), nullable=False),
    sa.Column('address', sa.String(), nullable=False),
    sa.Column('latitude', sa.Float(), nullable=True),
    sa.Column('longitude', sa.Float(), nullable=True),
    sa.Column('city', sa.String(), nullable=True),
    sa.Column('state', sa.String(), nullable=True),
    sa.Column('country', sa.String(), nullable=True),
    sa.Column('response_full', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('num_errors', sa.Integer(), nullable=True),
    sa.Column('date_added', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('provider', 'address_id')
    )


def downgrade() -> None:
    op.drop_table('geocode')
