"""creating netzero startup table

Revision ID: cb081e639c14
Revises: 18a43e1168f4
Create Date: 2025-04-30 15:42:17.168899

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cb081e639c14'
down_revision: Union[str, None] = '18a43e1168f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('startups',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('client_id', sa.Integer(), nullable=True),
    sa.Column('name', sa.String(), nullable=True),
    sa.Column('logo', sa.String(), nullable=True),
    sa.Column('website', sa.String(), nullable=True),
    sa.Column('domain', sa.String(), nullable=True),
    sa.Column('pitch_line', sa.String(), nullable=True),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('funding_amount', sa.Integer(), nullable=True),
    sa.Column('funding_string', sa.String(), nullable=True),
    sa.Column('funding_amount_usd', sa.Integer(), nullable=True),
    sa.Column('funding_string_usd', sa.String(), nullable=True),
    sa.Column('funding_range_id', sa.Integer(), nullable=True),
    sa.Column('funding_range', sa.String(), nullable=True),
    sa.Column('funding_range_usd', sa.String(), nullable=True),
    sa.Column('funding_range_id_usd', sa.Integer(), nullable=True),
    sa.Column('last_round_amount', sa.Integer(), nullable=True),
    sa.Column('last_round_amount_usd', sa.Integer(), nullable=True),
    sa.Column('last_round_amount_string', sa.String(), nullable=True),
    sa.Column('last_round_amount_string_usd', sa.String(), nullable=True),
    sa.Column('last_round_type', sa.String(), nullable=True),
    sa.Column('round_count', sa.Integer(), nullable=True),
    sa.Column('number_of_equity_rounds', sa.Integer(), nullable=True),
    sa.Column('number_of_grants', sa.Integer(), nullable=True),
    sa.Column('last_round_date', sa.DateTime(), nullable=True),
    sa.Column('acquisition_date', sa.DateTime(), nullable=True),
    sa.Column('founded_date', sa.Integer(), nullable=True),
    sa.Column('review_date', sa.DateTime(), nullable=True),
    sa.Column('last_seen_date', sa.DateTime(), nullable=True),
    sa.Column('georow_id', sa.Integer(), nullable=True),
    sa.Column('country_id', sa.Integer(), nullable=True),
    sa.Column('country', sa.String(), nullable=True),
    sa.Column('city', sa.String(), nullable=True),
    sa.Column('continent', sa.String(), nullable=True),
    sa.Column('email', sa.String(), nullable=True),
    sa.Column('phone', sa.String(), nullable=True),
    sa.Column('linkedin_url', sa.String(), nullable=True),
    sa.Column('twitter_url', sa.String(), nullable=True),
    sa.Column('facebook_url', sa.String(), nullable=True),
    sa.Column('direct_url', sa.String(), nullable=True),
    sa.Column('size_id', sa.Integer(), nullable=True),
    sa.Column('size', sa.String(), nullable=True),
    sa.Column('stage_id', sa.Integer(), nullable=True),
    sa.Column('stage', sa.String(), nullable=True),
    sa.Column('sustainability_metric', sa.Float(), nullable=True),
    sa.Column('sustainability_metric_id', sa.Integer(), nullable=True),
    sa.Column('sustainability_metric_label', sa.String(), nullable=True),
    sa.Column('revenues_range', sa.String(), nullable=True),
    sa.Column('current_employees_count', sa.Integer(), nullable=True),
    sa.Column('employees_growth_json', sa.JSON(), nullable=True),
    sa.Column('eutopia_score', sa.Integer(), nullable=True),
    sa.Column('active', sa.Boolean(), nullable=True),
    sa.Column('note', sa.String(), nullable=True),
    sa.Column('last_reviewer', sa.String(), nullable=True),
    sa.Column('tags', sa.JSON(), nullable=True),
    sa.Column('trls', sa.JSON(), nullable=True),
    sa.Column('sdgs', sa.JSON(), nullable=True),
    sa.Column('pi_frameworks', sa.JSON(), nullable=True),
    sa.Column('funding_types', sa.JSON(), nullable=True),
    sa.Column('date_added', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('client_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('startups')
    # ### end Alembic commands ###
