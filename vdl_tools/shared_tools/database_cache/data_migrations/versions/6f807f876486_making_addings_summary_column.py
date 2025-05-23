"""making addings summary column

Revision ID: 6f807f876486
Revises: 8dd63f06d7d2
Create Date: 2025-04-01 13:21:49.922333

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6f807f876486'
down_revision: Union[str, None] = '8dd63f06d7d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('web_pages_parsed', sa.Column('home_url', sa.String(), nullable=False))
    op.create_index(op.f('ix_web_pages_parsed_home_url'), 'web_pages_parsed', ['home_url'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_web_pages_parsed_home_url'), table_name='web_pages_parsed')
    op.drop_column('web_pages_parsed', 'home_url')
    # ### end Alembic commands ###
