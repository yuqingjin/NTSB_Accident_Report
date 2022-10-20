from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

Base = declarative_base()

class Accidents(Base):
    __tablename__ = "accidents"
    cm_mkey = Column(Integer, primary_key=True)
    cm_closed = Column(String)
    cm_completionStatus = Column(String)
    cm_hasSafetyRec = Column(String)
    cm_highestInjury = Column(String)

    cm_mode = Column(String)
    cm_ntsbNum = Column(String)
    cm_mostRecentReportType = Column(String)
    cm_probableCause = Column(String)
    cm_city = Column(String)

    cm_country = Column(String)
    cm_eventDate = Column(String)
    cm_state = Column(String)
    cm_eventType = Column(String)
    airportId = Column(String)

    airportName = Column(String)
    analysisNarrative = Column(String)
    cm_fatalInjuryCount = Column(Integer)
    cm_minorInjuryCount = Column(Integer)
    cm_seriousInjuryCount = Column(Integer)


class Vehicles(Base):
    __tablename__ = "vehicles"
    registrationNumber = Column(String, primary_key=True)
    SerialNumber = Column(String)
    aircraftCategory = Column(String)
    make = Column(String)
    model = Column(String)


class AccidentVehicle(Base):
    __tablename__ = "accident_vehicle"
    cm_mkey = Column('cm_mkey', Integer, 
                     ForeignKey('accidents.cm_mkey'), 
                     primary_key=True)
    registrationNumber = Column('registrationNumber', String, 
                                ForeignKey('vehicles.registrationNumber'), 
                                primary_key=True)
    DamageLevel = Column(String)
    ExplosionType = Column(String)
    FireType = Column(String)
    numberOfEngines = Column(Integer)
    airMedical = Column(String)

    operatorName = Column(String)
    registeredOwner = Column(String)
    regulationFlightConductedUnder = Column(String)


class Events(Base):
    __tablename__ = "events"
    cm_mkey = Column('cm_mkey', Integer, 
                     ForeignKey('accidents.cm_mkey'), 
                     primary_key=True)
    registrationNumber = Column('registrationNumber', String, 
                                ForeignKey('vehicles.registrationNumber'), 
                                primary_key=True)
    cm_eventCode = Column(String, primary_key=True)
    cicttEventSOEGroup = Column(String)
    cicttPhaseSOEGroup = Column(String)
    cm_isDefiningEvent = Column(String)
    cm_sequenceNum = Column(Integer)
