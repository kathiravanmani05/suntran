from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Batch2Input1(Base):
    __tablename__ = 'batch1'
    
    from_alternateId = Column(String, primary_key=True)
    to_alternateId = Column(String, primary_key=True)
    CODE =  Column(String)
    route_start =  Column(String)
    route_dest =  Column(String)
    pax_1 = Column(Integer)
    pax_2 = Column(Integer)
    pax_3 = Column(Integer)
    pax_4 = Column(Integer)
    pax_5 = Column(Integer)
    pax_6 = Column(Integer)
    pax_7 = Column(Integer)
    pax_8 = Column(Integer)
    pax_9 = Column(Integer)
    pax_10 = Column(Integer)
    pax_11 = Column(Integer)
    pax_12 = Column(Integer)
    pax_13 = Column(Integer)
    pax_14 = Column(Integer)
    pax_15 = Column(Integer)
    pax_16 = Column(Integer)
    Retry = Column(Integer, default=0)
    status = Column(Integer)

   