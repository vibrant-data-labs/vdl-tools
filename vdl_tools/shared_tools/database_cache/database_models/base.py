import datetime as dt
from sqlalchemy import Column, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BaseMixin(Base):
    __abstract__ = True

    date_added = Column(DateTime, default=dt.datetime.utcnow,  server_default=func.now())
    date_updated = Column(DateTime, onupdate=dt.datetime.utcnow)
    

    def to_dict(self):
        return {field.name:getattr(self, field.name) for field in self.__table__.c}
