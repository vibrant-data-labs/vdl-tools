from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, BigInteger, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from sqlalchemy_utils import generic_repr


@generic_repr
class StartupFundingRounds(BaseMixin):
    """
    Model for a startup in the NetZero Insights database.
    GET /fundingRound/prints/[clientID]

    """
    __tablename__ = 'startup_funding_rounds_nzi'

    id = Column(Integer, primary_key=True)
    clientId = Column(Integer, ForeignKey("startups_nzi.clientID"), index=True)
    fundingRoundID = Column(Integer, index=True)

    roundDate = Column(DateTime)
    roundType = Column(String)
    roundCurrency = Column(String)
    roundAmount = Column(Float)
    roundAmountUSD = Column(Float)
    roundAmountID = Column(Integer)
    coFundingRoundID = Column(Integer)
    originalRoundAmount = Column(Float)
    equityStageID = Column(Integer)
    exitStageID = Column(Integer)
    fundingRange = Column(String)
    financingType = Column(String)
    roundNewsIDs = Column(JSONB)
    roundInvestorIDs = Column(JSONB)
    roundNews = Column(JSONB)
    roundInvestors = Column(JSONB)
    valuationOriginalCurrency = Column(String)
    valuationAmount = Column(Float)
    valuationCurrency = Column(String)
    valuationType = Column(String)
    valuationAmountInOriginalCurrency = Column(Float)
    hideValuation = Column(Boolean)
    source = Column(String)
    status = Column(String)
