"""adding indexes to prompts and scraped websites

Revision ID: 18a43e1168f4
Revises: 28e94a837b5a
Create Date: 2025-04-02 09:13:09.564558

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '18a43e1168f4'
down_revision: Union[str, None] = '28e94a837b5a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_prompt_id'), 'prompt', ['id'], unique=False)
    op.create_index(op.f('ix_prompt_response_given_id'), 'prompt_response', ['given_id'], unique=False)
    op.create_index(op.f('ix_prompt_response_prompt_id'), 'prompt_response', ['prompt_id'], unique=False)
    op.create_index(op.f('ix_prompt_response_text_id'), 'prompt_response', ['text_id'], unique=False)
    op.create_index(op.f('ix_web_pages_scraped_cleaned_key'), 'web_pages_scraped', ['cleaned_key'], unique=False)
    op.create_index(op.f('ix_web_pages_scraped_page_type'), 'web_pages_scraped', ['page_type'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_web_pages_scraped_page_type'), table_name='web_pages_scraped')
    op.drop_index(op.f('ix_web_pages_scraped_cleaned_key'), table_name='web_pages_scraped')
    op.drop_index(op.f('ix_prompt_response_text_id'), table_name='prompt_response')
    op.drop_index(op.f('ix_prompt_response_prompt_id'), table_name='prompt_response')
    op.drop_index(op.f('ix_prompt_response_given_id'), table_name='prompt_response')
    op.drop_index(op.f('ix_prompt_id'), table_name='prompt')
    # ### end Alembic commands ###
