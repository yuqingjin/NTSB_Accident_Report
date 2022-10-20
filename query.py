import pandas as pd
import sqlite3

def search_closed_in_n_month(start_date, end_date):
    # shape the input date into the string format in database
    start_date_convert = start_date + "T00:00:00Z"
    end_date_convert = end_date + "T23:59:59Z"

    # read sqlite query results into a pandas DataFrame
    conn = sqlite3.connect("ntsb.db")
    # query those closed accidents in past serveral months
    query = """SELECT *
    FROM ((accidents INNER JOIN accident_vehicle ON accidents.cm_mkey = accident_vehicle.cm_mkey) 
    INNER JOIN vehicles ON accident_vehicle.registrationNumber = vehicles.registrationNumber) 
    INNER JOIN events ON events.cm_mkey = accidents.cm_mkey
    WHERE accidents.cm_closed = 1 AND accidents.cm_eventdate BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY vehicles.registrationNumber
    ORDER BY accidents.cm_eventDate""".format(start_date = start_date_convert, end_date = end_date_convert)

    df = pd.read_sql_query(query, conn)

    # verify that result of SQL query is stored in the dataframe
    print("Here is the first 20 rows in accidents result")
    print(df.head(20))
    csv_name = 'ClosedAccidentBetween_{start_date}_and_{end_date}.csv'.format(start_date = start_date, end_date = end_date)
    df.to_csv(csv_name, index = True)
    conn.close()
    return


def search_fatalInjuryMoreThanOne_in_n_month(start_date, end_date):
    # shape the input date into the string format in database
    start_date_convert = start_date + "T00:00:00Z"
    end_date_convert = end_date + "T23:59:59Z"

    # read sqlite query results into a pandas DataFrame
    conn = sqlite3.connect("ntsb.db")
    # query those closed accidents in past serveral months
    query = """SELECT *
    FROM ((accidents INNER JOIN accident_vehicle ON accidents.cm_mkey = accident_vehicle.cm_mkey) 
    INNER JOIN vehicles ON accident_vehicle.registrationNumber = vehicles.registrationNumber) 
    INNER JOIN events ON events.cm_mkey = accidents.cm_mkey
    WHERE accidents.cm_fatalInjuryCount >= 1 AND accidents.cm_eventdate BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY vehicles.registrationNumber
    ORDER BY accidents.cm_eventDate""".format(start_date = start_date_convert, end_date = end_date_convert)

    df = pd.read_sql_query(query, conn)

    # verify that result of SQL query is stored in the dataframe
    print("Here is the first 20 rows in accidents result")
    print(df.head(20))
    csv_name = 'FatalInjuryAccidentBetween_{start_date}_and_{end_date}.csv'.format(start_date = start_date, end_date = end_date)
    df.to_csv(csv_name, index = True)
    conn.close()
    return
