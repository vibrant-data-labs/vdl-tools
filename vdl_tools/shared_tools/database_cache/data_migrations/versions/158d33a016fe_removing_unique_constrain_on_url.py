"""removing unique constrain on url

Revision ID: 158d33a016fe
Revises: 3759d9b2bed9
Create Date: 2024-04-15 17:20:05.837575

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '158d33a016fe'
down_revision: Union[str, None] = '3759d9b2bed9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_linkedin_organization_url', table_name='linkedin_organization')
    op.create_index(op.f('ix_linkedin_organization_url'), 'linkedin_organization', ['url'], unique=False)
    op.alter_column('prompt_response', 'text_id',
               existing_type=sa.VARCHAR(),
               nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('prompt_response', 'text_id',
               existing_type=sa.VARCHAR(),
               nullable=False)
    op.drop_index(op.f('ix_linkedin_organization_url'), table_name='linkedin_organization')
    op.create_index('ix_linkedin_organization_url', 'linkedin_organization', ['url'], unique=True)
    # ### end Alembic commands ###
