from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, BigInteger
from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from sqlalchemy_utils import generic_repr


@generic_repr
class Startup(BaseMixin):
    __tablename__ = 'startups'

    # Primary and unique identifiers
    id = Column(Integer, primary_key=True)
    clientID = Column(Integer, unique=True)
    name = Column(String)
    alternativeNames = Column(JSON)  # Alternative company names
    legalNames = Column(JSON)  # Legal company names

    # Company presentation
    logo = Column(String)
    website = Column(String)
    domain = Column(String)
    pitchLine = Column(String)
    description = Column(String)
    uniqueSellingProposition = Column(String)
    pitchdeckURL = Column(String)  # URL to the company pitch deck

    # Contact information
    email = Column(String)
    phone = Column(String)
    linkedinURL = Column(String)
    twitterURL = Column(String)
    facebookURL = Column(String)  # URL to company Facebook
    directURL = Column(String)

    # Location information
    address = Column(String)
    city = Column(String)
    cityID = Column(Integer)  # City ID
    admin1 = Column(String)
    admin2 = Column(String)
    admin3 = Column(String)
    admin4 = Column(String)
    admin1ID = Column(Integer)
    admin2ID = Column(Integer)
    admin3ID = Column(Integer)
    admin4ID = Column(Integer)
    georowID = Column(Integer)
    countryID = Column(Integer)
    country = Column(String)
    countryCode = Column(String)
    continent = Column(String)
    continentID = Column(Integer)  # Continent ID

    # Dates
    foundedDate = Column(Integer)  # Year only
    acquisitionDate = Column(String)  # ISO format string
    lastRoundDate = Column(String)  # ISO format string
    reviewDate = Column(String)  # ISO format string
    lastSeenDate = Column(String)  # ISO format string
    dealsReviewDate = Column(DateTime)
    trlAcquisitionDate = Column(String)  # ISO format string
    currentlyFundraisingDate = Column(String)  # Date of estimation

    # Funding information
    fundingAmount = Column(Float)
    fundingString = Column(String)
    fundingAmountUSD = Column(Float)
    fundingStringUSD = Column(String)
    fundingRangeID = Column(Integer)
    fundingRange = Column(String)
    fundingRangeUSD = Column(String)
    fundingRangeIDUSD = Column(Integer)
    lastRoundAmount = Column(BigInteger)
    lastRoundAmountUSD = Column(BigInteger)
    lastRoundAmountString = Column(String)
    lastRoundAmountStringUSD = Column(String)
    lastRoundType = Column(String)
    roundCount = Column(Integer)
    roundWithDateCount = Column(Integer)  # Number of rounds with a date available
    numberOfEquityRounds = Column(Integer)
    numberOfDebtRounds = Column(Integer)  # Number of company deals of type Debt
    numberOfGrants = Column(Integer)  # Number of company deals of type Grant
    lastPostMoneyValuation = Column(Float)  # Latest post money valuation
    lastPostMoneyValuationCurrency = Column(String)  # Latest post money valuation currency
    fundRaising = Column(Boolean)
    isFundRaising = Column(Boolean)  # Company likelihood to fundraise
    currentlyFundraising = Column(Boolean)  # Estimation about the company being currently fundraising
    dealsLastReviewer = Column(String)

    # Revenue information
    revenueEuro = Column(BigInteger)
    revenueYear = Column(Integer)
    revenuesRangeID = Column(Integer)
    linkedInRevenuesRangeID = Column(Integer)
    revenuesRange = Column(String)

    # Company metrics
    sizeID = Column(Integer)
    size = Column(String)
    stageID = Column(Integer)
    stage = Column(String)
    currentEmployeesCount = Column(Integer)
    employeesGrowthJSON = Column(String)  # JSON string
    eutopiaScore = Column(Integer)

    # Growth metrics
    yoYEmployeesGrowth = Column(Float)
    qoQEmployeesGrowth = Column(Float)
    yoYCorrespondingQuarter = Column(String)
    qoQCorrespondingQuarter = Column(String)

    # Sustainability and impact
    sustainabilityMetric = Column(Float)
    sustainabilityMetricID = Column(Integer)
    sustainabilityMetricLabel = Column(String)
    impact = Column(String)  # Company impact on climate

    # TRL information
    trl = Column(JSON)  # Dictionary
    trlID = Column(Integer)

    # Status and notes
    active = Column(Boolean)
    note = Column(String)
    lastReviewer = Column(String)
    intellectualProperty = Column(Boolean)  # If true, the company has some registered IP

    # List data as JSON fields
    tags = Column(JSON)  # List of tag objects
    piFrameworks = Column(JSON)  # List of PI framework objects
    fundingTypes = Column(JSON)  # List of funding type strings
    sdgs = Column(JSON)  # List of SDG objects
    actorTypes = Column(JSON)  # List of actor types
    investorPartnerSet = Column(JSON)  # Set of investorIDs
    companyContactSet = Column(JSON)  # Set of contact IDs

    # Additional fields
    tbFinancialStage = Column(JSON)  # Dictionary
    tagSourceMap = Column(JSON)  # Dictionary