import requests
import json
import sys
from zipfile import ZipFile
from sqlalchemy import delete

from config import *
from db_object import *
from query import *


def queryTotalSize(start_date, end_date):
    query_body["QueryGroups"][0]["QueryRules"][0]["Values"] = [start_date, end_date]
    # print("query_body", query_body)
    rsp = requests.post(query_url, data=json.dumps(query_body), headers=headers).json()
    # print(type(rsp))
    # rsp = requests.post(query_url, data=json.dumps(query_body), headers=headers).json()
    cnt = rsp["ResultListCount"]
    print("The result has ", str(cnt), " records.")
    return cnt


def download(start_date, end_date, queryType):
    start_date = str(start_date)
    end_date = str(end_date)
    total = queryTotalSize(start_date, end_date)
    offset = 0
    zips = []
    query_body["QueryGroups"][0]["QueryRules"][0]["Values"] = [start_date, end_date]
    for i in range(total//page_size + 1): # download data in zips
        query_body["ResultSetOffset"] = offset
        rsp = requests.post(download_url, data=json.dumps(query_body), headers=headers)
        # print(type(rsp))
        fname = f'{start_date}_{end_date}_{queryType}_{offset}.zip'
        with open(fname,'wb') as f:
            f.write(rsp.content)
        zips.append(fname[:-4])
        offset += 1
    return zips


# unzip files into json format
def unzipfiles(zips):
    for zip in zips:
        # print(zip)
        zipdata = ZipFile(f'{zip}.zip')
        # iterate through each file
        for i, f in enumerate(zipdata.filelist):
            if f.filename[-4:] == "json":
                json_name = zip + ".json"
                f.filename = json_name
                zipdata.extract(f)
    return


engine = None
DBSession = scoped_session(sessionmaker())
def init_sqlalchemy(dbname='sqlite:///ntsb.db'):
    global engine
    engine = create_engine(dbname, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def insert_data_into_tables(zips):
    init_sqlalchemy()

    for zip in zips:
        file = open('./{z}.json'.format(z = zip))
        data = json.load(file)

        # accident_entries = []
        for d in data:
            # insert accident data
            cm_mkey = d['cm_mkey']

            # in case there are records with the same cm_mkey existing in db
            # delete it before insert new data
            exist = DBSession.query(Accidents).filter(Accidents.cm_mkey == cm_mkey).all()
            if len(exist) >= 1:
                dlt = delete(Accidents).where(Accidents.cm_mkey == cm_mkey)
                DBSession.execute(dlt)
                DBSession.commit()

            new_acc_entry = Accidents(cm_mkey = d['cm_mkey'],
                                  cm_closed = d['cm_closed'] if 'cm_closed' in d else None,
                                  cm_completionStatus = d['cm_completionStatus'] if 'cm_completionStatus' in d else None,
                                  cm_hasSafetyRec = d['cm_hasSafetyRec'] if 'cm_hasSafetyRec' in d else None,
                                  cm_highestInjury = d['cm_highestInjury'] if 'cm_highestInjury' in d else None,

                                  cm_mode = d['cm_mode'] if 'cm_mode' in d else None,
                                  cm_ntsbNum = d['cm_ntsbNum'] if "cm_ntsbNum" in d else None,
                                  cm_mostRecentReportType = d['cm_mostRecentReportType'] if 'cm_mostRecentReportType' in d else None,
                                  cm_probableCause = d['cm_probableCause'] if 'cm_probableCause' in d else None,
                                  cm_city = d['cm_city'] if "cm_city" in d else None,

                                  cm_country = d['cm_country'] if 'cm_country' in d else None,
                                  cm_eventDate = d['cm_eventDate'] if 'cm_eventDate' in d else None,
                                  cm_state = d['cm_state'] if 'cm_state' in d else None,
                                  cm_eventType = d['cm_eventType'] if 'cm_eventType' in d else None,
                                  airportId = d['airportId'] if 'airportId' in d else None,

                                  airportName = d['airportName'] if 'airportName' in d else None,
                                  analysisNarrative = d['analysisNarrative'] if "analysisNarrative" in d else None,
                                  cm_fatalInjuryCount = d['cm_fatalInjuryCount'] if "cm_fatalInjuryCount" in d else 0,
                                  cm_minorInjuryCount = d['cm_minorInjuryCount'] if 'cm_minorInjuryCount' in d else 0,
                                  cm_seriousInjuryCount = d['cm_seriousInjuryCount'] if 'cm_seriousInjuryCount' in d else 0)
            # accident_entries.append(new_acc_entry)
            DBSession.add(new_acc_entry)
            DBSession.commit()

            if "cm_vehicles" in d:
                vehicles = d["cm_vehicles"]
                if len(vehicles) != 0:
                    # vehicle_entries = []
                    # acc_vehicle_entries = []
                    for v in vehicles:
                        registrationNumber = v["registrationNumber"]
                        # print("registrationNumber", registrationNumber)
                        if registrationNumber:
                            # in case the registration Number already exists in db
                            exist = DBSession.query(Vehicles).filter(Vehicles.registrationNumber == registrationNumber).all()
                            # print("if exists another vehicle", exist)
                            if len(exist) >= 1:
                                # print("Yes, it exists and you need to delete it before insert")
                                dlt = delete(Vehicles).where(Vehicles.registrationNumber == registrationNumber)
                                # dlt_in_accVeh = delete(AccidentVehicle).where(AccidentVehicle.registrationNumber == registrationNumber)
                                DBSession.execute(dlt)
                                # DBSession.execute(dlt_in_accVeh)
                                DBSession.commit()
                            

                            # insert vehicles data
                            new_vehicle_entry = Vehicles(registrationNumber = v["registrationNumber"],
                                            SerialNumber = v["SerialNumber"] if "SerialNumber" in v else None,
                                            aircraftCategory = v["aircraftCategory"] if "aircraftCategory" in v else None,
                                            make = v["make"] if "make" in v else None,
                                            model = v["model"] if "model" in v else None,
                                            )
                            # vehicle_entries.append(new_vehicle_entry)
                            DBSession.add(new_vehicle_entry)
                            DBSession.commit()


                            # insert AccidentVehicles data
                            new_accVehi_entry = AccidentVehicle(cm_mkey = cm_mkey,
                                            registrationNumber = registrationNumber,
                                            DamageLevel = v["DamageLevel"] if "DamageLevel" in v else None,
                                            ExplosionType = v["ExplosionType"] if "ExplosionType" in v else None,
                                            FireType = v["FireType"] if "FireType" in v else None,
                                            numberOfEngines = v["numberOfEngines"] if "numberOfEngines" in v else 0,
                                            airMedical = v["airMedical"] if "airMedical" in v else None,
                                            operatorName = v["operatorName"] if "operatorName" in v else None,
                                            registeredOwner = v["registeredOwner"] if "registeredOwner" in v else None,
                                            regulationFlightConductedUnder = v["regulationFlightConductedUnder"] if "regulationFlightConductedUnder" in v else None
                                                        )
                            # acc_vehicle_entries.append(new_accVehi_entry)
                            DBSession.add(new_accVehi_entry)
                            DBSession.commit()

                            if 'cm_events' in v:
                                events = v["cm_events"]
                                # event_entries = []
                                for e in events:

                                    new_event_entry = Events(cm_mkey = cm_mkey,
                                                    registrationNumber = registrationNumber,
                                                    cm_eventCode = e["cm_eventCode"],
                                                    cicttEventSOEGroup = e["cicttEventSOEGroup"] if "cicttEventSOEGroup" in e else None,
                                                    cicttPhaseSOEGroup = e["cicttPhaseSOEGroup"] if "cicttPhaseSOEGroup" in e else None,
                                                    cm_isDefiningEvent = e["cm_isDefiningEvent"] if "cm_isDefiningEvent" in e else None,
                                                    cm_sequenceNum = e["cm_sequenceNum"] if "cm_sequenceNum" in e else None
                                                    )

                                    # event_entries.append(new_event_entry)
                                    DBSession.add(new_event_entry)
                                    DBSession.commit()

                #     DBSession.add_all(event_entries)
            
                # DBSession.add_all(vehicle_entries)
                # DBSession.add_all(acc_vehicle_entries)

        # DBSession.add_all(accident_entries)

    DBSession.commit()
    print("Insert data into database successfully!")


def fetch(start_date, end_date, queryType):
    zips = download(start_date, end_date, queryType)
    unzipfiles(zips)
    insert_data_into_tables(zips)
    return


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('usage: fetch <start_date>(yyyy-mm-dd) [end_date](yyyy-mm-dd) [queryType]("closed" or "fatal")')
    
    else:
        start_date = str(sys.argv[1])
        end_date = str(sys.argv[2])
        engine = None
        DBSession = scoped_session(sessionmaker())

        if str(sys.argv[3]) == "closed":
            fetch(start_date, end_date, "closed")
            search_closed_in_n_month(start_date, end_date)

        elif str(sys.argv[3]) == "fatal":
            fetch(start_date, end_date, "fatal")
            search_fatalInjuryMoreThanOne_in_n_month(start_date, end_date)

        else:
            print('queryType error: should be "closed" or "fatal"')

    
