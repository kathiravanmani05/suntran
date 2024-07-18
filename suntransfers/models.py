from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Sequence, UniqueConstraint,Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Route(Base):
    __tablename__ = 'routes'


    serial_no = Column(Integer, Sequence('route_id_seq'), autoincrement=True,primary_key=True)
    Competitor = Column(String(100))
    key = Column(String(100))
    status = Column(Boolean)
    retry = Column(Integer,default=0)
    Route_start = Column(String(255))
    from_alternateId = Column(Integer)
    code = Column(String(10))
    Route_dest = Column(String(255))
    to_alternateId = Column(Integer)
    pax_1 = Column(Float)
    pax_2 = Column(Float)
    pax_3 = Column(Float)
    pax_4 = Column(Float)
    pax_5 = Column(Float)
    pax_6 = Column(Float)
    pax_7 = Column(Float)
    pax_8 = Column(Float)
    pax_9 = Column(Float)
    pax_10 = Column(Float)
    pax_11 = Column(Float)
    pax_12 = Column(Float)
    pax_13 = Column(Float)
    pax_14 = Column(Float)
    pax_15 = Column(Float)
    pax_16 = Column(Float)

    __table_args__ = (UniqueConstraint('key', name='uq_key'),)
   