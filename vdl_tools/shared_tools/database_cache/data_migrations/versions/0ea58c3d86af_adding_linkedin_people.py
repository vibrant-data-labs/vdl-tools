"""adding linkedin people

Revision ID: 0ea58c3d86af
Revises: 8b77bcd64de3
Create Date: 2024-06-25 15:06:59.577797

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0ea58c3d86af'
down_revision: Union[str, None] = '8b77bcd64de3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('linkedin_person',
    sa.Column('linkedin_id', sa.String(), nullable=False),
    sa.Column('original_id', sa.String(), nullable=True),
    sa.Column('name', sa.String(), nullable=True),
    sa.Column('first_name', sa.String(), nullable=True),
    sa.Column('last_name', sa.String(), nullable=True),
    sa.Column('title', sa.String(), nullable=True),
    sa.Column('url', sa.String(), nullable=True),
    sa.Column('location', sa.String(), nullable=True),
    sa.Column('industry', sa.String(), nullable=True),
    sa.Column('summary', sa.String(), nullable=True),
    sa.Column('connections', sa.String(), nullable=True),
    sa.Column('logo_url', sa.String(), nullable=True),
    sa.Column('country', sa.String(), nullable=True),
    sa.Column('connections_count', sa.Integer(), nullable=True),
    sa.Column('experience_count', sa.Integer(), nullable=True),
    sa.Column('member_shorthand_name', sa.String(), nullable=True),
    sa.Column('member_shorthand_name_hash', sa.String(), nullable=True),
    sa.Column('canonical_url', sa.String(), nullable=True),
    sa.Column('canonical_hash', sa.String(), nullable=True),
    sa.Column('canonical_shorthand_name', sa.String(), nullable=True),
    sa.Column('canonical_shorthand_name_hash', sa.String(), nullable=True),
    sa.Column('member_also_viewed_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_awards_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_certifications_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_education_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_experience_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_languages_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_organizations_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_patent_status_list', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_projects_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_publications_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_volunteering_positions_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('member_websites_collection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('datasource', sa.String(), nullable=True),
    sa.Column('raw_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('num_errors', sa.Integer(), nullable=True),
    sa.Column('date_added', sa.DateTime(), nullable=True),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('linkedin_id')
    )
    op.create_index(op.f('ix_linkedin_person_datasource'), 'linkedin_person', ['datasource'], unique=False)
    op.create_index(op.f('ix_linkedin_person_url'), 'linkedin_person', ['url'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_linkedin_person_url'), table_name='linkedin_person')
    op.drop_index(op.f('ix_linkedin_person_datasource'), table_name='linkedin_person')
    op.drop_table('linkedin_person')
    # ### end Alembic commands ###
