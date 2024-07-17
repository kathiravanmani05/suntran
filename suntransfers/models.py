from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Sequence, UniqueConstraint
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

    __table_args__ = (UniqueConstraint('key', name='uq_key'),)
   