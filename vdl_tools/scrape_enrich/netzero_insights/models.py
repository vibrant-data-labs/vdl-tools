from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON
from sqlalchemy.orm import declarative_base
from datetime import datetime
from typing import Optional, List, Dict, Any

Base = declarative_base()

class Startup(Base):
    __tablename__ = 'startups'
    
    # Basic information
    id = Column(Integer, primary_key=True)
    clientID = Column(Integer, unique=True)
    name = Column(String)
    logo = Column(String)
    website = Column(String)
    domain = Column(String)
    pitchLine = Column(String)
    description = Column(String)
    
    # Funding information
    fundingAmount = Column(Float)
    fundingString = Column(String)
    fundingAmountUSD = Column(Float)
    fundingStringUSD = Column(String)
    fundingRangeID = Column(Integer)
    fundingRange = Column(String)
    fundingRangeUSD = Column(String)
    fundingRangeIDUSD = Column(Integer)
    lastRoundAmount = Column(Integer)
    lastRoundAmountUSD = Column(Integer)
    lastRoundAmountString = Column(String)
    lastRoundAmountStringUSD = Column(String)
    lastRoundType = Column(String)
    roundCount = Column(Integer)
    numberOfEquityRounds = Column(Integer)
    fundRaising = Column(Boolean)
    
    # Revenue information
    revenueEuro = Column(Integer)
    revenueYear = Column(Integer)
    revenuesRangeID = Column(Integer)
    linkedInRevenuesRangeID = Column(Integer)
    revenuesRange = Column(String)
    
    # Dates
    lastRoundDate = Column(String)  # ISO format string
    acquisitionDate = Column(String)  # ISO format string
    foundedDate = Column(Integer)  # Year only
    reviewDate = Column(String)  # ISO format string
    lastSeenDate = Column(String)  # ISO format string
    
    # Location information
    georowID = Column(Integer)
    countryID = Column(Integer)
    country = Column(String)
    countryCode = Column(String)
    city = Column(String)
    continent = Column(String)
    
    # Contact information
    linkedinURL = Column(String)
    twitterURL = Column(String)
    directURL = Column(String)
    
    # Company metrics
    sizeID = Column(Integer)
    size = Column(String)
    stageID = Column(Integer)
    stage = Column(String)
    sustainabilityMetric = Column(Float)
    sustainabilityMetricID = Column(Integer)
    sustainabilityMetricLabel = Column(String)
    currentEmployeesCount = Column(Integer)
    employeesGrowthJSON = Column(String)  # JSON string
    eutopiaScore = Column(Integer)
    
    # Growth metrics
    yoYEmployeesGrowth = Column(Float)
    qoQEmployeesGrowth = Column(Float)
    yoYCorrespondingQuarter = Column(String)
    qoQCorrespondingQuarter = Column(String)
    
    # TRL information
    trl = Column(JSON)  # Dictionary
    trlID = Column(Integer)
    trlAcquisitionDate = Column(String)  # ISO format string
    
    # Status
    active = Column(Boolean)
    note = Column(String)
    lastReviewer = Column(String)
    
    # List data as JSON fields
    tags = Column(JSON)  # List of tag objects
    piFrameworks = Column(JSON)  # List of PI framework objects
    fundingTypes = Column(JSON)  # List of funding type strings
    sdgs = Column(JSON)  # List of SDG objects
    actorTypes = Column(JSON)  # List of actor types
    
    # Additional fields
    tbFinancialStage = Column(JSON)  # Dictionary
    tagSourceMap = Column(JSON)  # Dictionary 