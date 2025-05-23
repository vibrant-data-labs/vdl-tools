"""fixing netzero startup table again

Revision ID: ce98e527ad87
Revises: bcc160f796fd
Create Date: 2025-05-01 13:37:37.794697

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ce98e527ad87'
down_revision: Union[str, None] = 'bcc160f796fd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('startups', sa.Column('alternativeNames', sa.JSON(), nullable=True))
    op.add_column('startups', sa.Column('legalNames', sa.JSON(), nullable=True))
    op.add_column('startups', sa.Column('uniqueSellingProposition', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('pitchdeckURL', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('email', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('phone', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('facebookURL', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('address', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('cityID', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('admin1', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('admin2', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('admin3', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('admin4', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('admin1ID', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('admin2ID', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('admin3ID', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('admin4ID', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('continentID', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('dealsReviewDate', sa.DateTime(), nullable=True))
    op.add_column('startups', sa.Column('currentlyFundraisingDate', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('roundWithDateCount', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('numberOfDebtRounds', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('numberOfGrants', sa.Integer(), nullable=True))
    op.add_column('startups', sa.Column('lastPostMoneyValuation', sa.Float(), nullable=True))
    op.add_column('startups', sa.Column('lastPostMoneyValuationCurrency', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('isFundRaising', sa.Boolean(), nullable=True))
    op.add_column('startups', sa.Column('currentlyFundraising', sa.Boolean(), nullable=True))
    op.add_column('startups', sa.Column('dealsLastReviewer', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('impact', sa.String(), nullable=True))
    op.add_column('startups', sa.Column('intellectualProperty', sa.Boolean(), nullable=True))
    op.add_column('startups', sa.Column('investorPartnerSet', sa.JSON(), nullable=True))
    op.add_column('startups', sa.Column('companyContactSet', sa.JSON(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('startups', 'companyContactSet')
    op.drop_column('startups', 'investorPartnerSet')
    op.drop_column('startups', 'intellectualProperty')
    op.drop_column('startups', 'impact')
    op.drop_column('startups', 'dealsLastReviewer')
    op.drop_column('startups', 'currentlyFundraising')
    op.drop_column('startups', 'isFundRaising')
    op.drop_column('startups', 'lastPostMoneyValuationCurrency')
    op.drop_column('startups', 'lastPostMoneyValuation')
    op.drop_column('startups', 'numberOfGrants')
    op.drop_column('startups', 'numberOfDebtRounds')
    op.drop_column('startups', 'roundWithDateCount')
    op.drop_column('startups', 'currentlyFundraisingDate')
    op.drop_column('startups', 'dealsReviewDate')
    op.drop_column('startups', 'continentID')
    op.drop_column('startups', 'admin4ID')
    op.drop_column('startups', 'admin3ID')
    op.drop_column('startups', 'admin2ID')
    op.drop_column('startups', 'admin1ID')
    op.drop_column('startups', 'admin4')
    op.drop_column('startups', 'admin3')
    op.drop_column('startups', 'admin2')
    op.drop_column('startups', 'admin1')
    op.drop_column('startups', 'cityID')
    op.drop_column('startups', 'address')
    op.drop_column('startups', 'facebookURL')
    op.drop_column('startups', 'phone')
    op.drop_column('startups', 'email')
    op.drop_column('startups', 'pitchdeckURL')
    op.drop_column('startups', 'uniqueSellingProposition')
    op.drop_column('startups', 'legalNames')
    op.drop_column('startups', 'alternativeNames')
    # ### end Alembic commands ###
