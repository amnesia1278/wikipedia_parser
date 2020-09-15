import wikipediaapi
import sqlite3
import sqlalchemy
import re
import datetime
from calendar import month_name



# Extract

def download_from_wiki(page, language = "en"):
    """
    :param page: page name
    :return: list for events, list for births, list for deaths
    """
    wiki_wiki = wikipediaapi.Wikipedia(language)


    page_py = wiki_wiki.page(page)
    page_list = page_py.text.split("\n")
    #print(page_list[0:5])
    index_events = page_list.index("Events")
    index_births = page_list.index("Births")
    index_deaths = page_list.index("Deaths")
    index_holidays = page_list.index("Holidays and observances")
    #print("Events: {}, births: {}, deaths: {}".format(index_events, index_births, index_deaths))

    list_events = page_list[index_events:index_births-1]
    list_births = page_list[index_births:index_deaths-1]
    list_deaths = page_list[index_deaths:index_holidays-1]

    list_events.append(page)
    list_births.append(page)
    list_deaths.append(page)
    #print(list_events[0:2])
    return list_events, list_births, list_deaths



# Transform

def get_all_days():
    """
    :return: list of all days needed for parsing Wikipedia in format "Month_dayNumber" e.g. "January_10"
    """
    start = datetime.datetime.strptime("01-01-2020", "%d-%m-%Y")
    end = datetime.datetime.strptime("01-01-2021", "%d-%m-%Y")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    #create a list with an appropriate date format
    list_of_all_days = []
    for date in date_generated:
        month = month_name[date.month]
        day = date.day
        altered_date = month+"_"+str(day)
        list_of_all_days.append(altered_date)
    return list_of_all_days


def split_string_into_name_and_description(s: str):
    """

    :param s: string
    :return: name and description for tables "birth" and "death"
    """
    # split name and description
    s = s.split(",", 1)

    #split and drop date in the end of the string
    if(len(s)== 1):
        # if there no description, get only name
        s_1 = s[0].split("(")
        name = s_1[0]
        description = "not described"
    else:
        # get both name and description
        s_1 = s[1].split("(")
        name = s[0]
        description = s_1[0]
        description = description[1:]
    return name, description


def check_year(year):
    """
    There are different cases for year format e.g. AD 98, 111 BC, 984, 1999 etc.
    All this formats have to be normalized.
    :param year: year
    :return: year in the right format and boolean flag bc
    """
    #("Begin: {}".format(year))
    bc = False
    if ("BC" in year):
        # Case march 17 death 45 BC
        bc = True
    """
    if ("BC" in year):
        # Case march 17 death 45 BC
        if len(year.split(" ")) >1:
            year = year.split(" ")[0]
        else:
            year = year[:-2]

        bc = True
    if ("AD" in year):
        year = year.split(" ")
        if year[0].isdigit():
            year = year[0]
        else:
            year = year[1]
    """
    # check if year smaller than 1000 and padding the year with 0,00 or 000

    year = re.sub("[^0-9]", "", year)
    #year = year.replace(" ", "")
    if (len(year) == 3):
        #print(year)
        year = "0" + year
    if (len(year) == 2):
        year = "00" + year
        #print(year)
    if (len(year) == 1):
        year = "000" + year
    # check if there is single year in the date
    # for the case Feb 2 1425 (or 1426) - the first date will be choosen
    if (len(year) >= 5):
        year = year[:4]

    #print("End: {}".format(year))
    return year, bc


def check_row(s):
    """

    :param s: string
    :return: flag is digit contained
    """
    contains_digit = False
    for character in s:
        if character.isdigit():
            contains_digit = True
    return contains_digit


def get_y_m_d_from_string(s):
    """

    :param s: string in format "YYYY-MM-DD HH:MM:SS" e.g. '2004-02-02 00:00:00'
    :return: year, month, day as integers e.g. 2004, 2, 2
    """
    s = str(s).split(" ")
    dt = s[0].split("-")
    year, month, day = int(dt[0]), int(dt[1]), int(dt[2])
    #print("Y,M,D: {}, {}, {}".format(year,month,day))
    return year, month, day


def gregorian_to_julian(year, month, day):
    """
    Transform gregorian date to julian
    :param year: year
    :param month: month
    :param day: day
    :return: date in julian format
    """

    i = int((month - 14) / 12)
    jd = day - 32075
    if year < 0:
        year = year + 1
    jd += int((1461 * (year + 4800 + i)) / 4)
    jd += int((367 * (month - 2 - (12 * i))) / 12)
    jd -= int((3 * int((year + 4900 + i) / 100)) / 4)
    jd -= 0.5
    return jd

# Load

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_connection(db_file = 'WikiDatabaseTest.sqlite3'):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def insert_artifact(conn, artifact, type):
    """
    Function inserts one artifact into db depending on the artifact's type (Events, Births, Deaths)
    :param conn: connection to db
    :param artifact: values that should be written in the DB
    :param type: type of occasion: event, birth or death
    :return: lastrow id
    """
    assert(type in ["Events", "Births", "Deaths"])

    if type == "Events":

        sql = ''' INSERT INTO event(date, date_julian, event_description)
                              VALUES(?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, artifact)
        conn.commit()
    elif type == "Births":
        sql = ''' INSERT INTO birth(date, date_julian, name, person_description)
                                      VALUES(?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, artifact)
        conn.commit()
    else:
        sql = ''' INSERT INTO death(date, date_julian, name, person_description)
                                              VALUES(?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, artifact)
        conn.commit()


def delete_dummy(conn):
    """
    Delete all headers and other infroamtion that was marked as dummy
    :param conn: DB connection
    :return:
    """

    sql = ''' DELETE FROM birth
                WHERE name = "dummy"; '''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    sql = ''' DELETE FROM death
                    WHERE name = "dummy"; '''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
# insert the whole list into db
def insert_data_into_db(list):
    """
    Take a list and insert data in the table according to the data type (Events, Births, Deaths)
    :param list:
    :return:
    """
    connection = create_connection()
    #get type of data and drop it from list
    type = list[0]
    list.pop(0)

    # get day
    day = list[-1]
    list.pop(-1)

    # case January_1 # drop all rows like "Pre-Julian Roman calendar"
    for item in list:
        if not check_row(item[:4]) and "calendar" in item or len(item) <1:
            i = list.index(item)
            list.pop(i)

    day = day.split("_")


    with connection:

        last_year = None

        for event in list:
            #print(event)
            event = event.replace(" - "," – ")
            # check case like January 27 year 1967 - death
            #print(index)
            if re.sub("[^0-9]", "", event[0:10]).isdigit() or "AD" in event[0:4]:
                year = event.split(' – ', 1)[0]
                #print("Test: {}".format(year))
                if len(event.split(' – ', 1)) == 1:
                    description = "dummy"
                else:
                    description = event.split(' – ')[1]
                last_year = year
                #print(year)
            else:
                year = last_year
                description = event

            # check diff cases of year
            year, bc = check_year(year)
            year = (year + "-" + day[0] + "-" + day[1]).replace(" ", "")
            year = datetime.datetime.strptime(year, '%Y-%B-%d')
            y,m,d = get_y_m_d_from_string(year)

            #transform year into integer format: YYYYMMDD
            year = (str(year).replace("-","")[:8])

            # if year BC, it will be represented as negative integer
            if bc == True:
                year = int("-" + year)
                y = y*(-1)

            julian = gregorian_to_julian(y, m, d)

            # different way to insert for different occasions
            if type == "Events":
                artifact = (year,julian, description)
            else:
                name, description = split_string_into_name_and_description(description)
                artifact = (year, julian, name, description)

            insert_artifact(connection, artifact, type)





if __name__ == '__main__':
    """
    Database was created in the SQLiteStudio. Therefore, this step is missed. 
    
    """
    sql_create_events_table = """ CREATE TABLE IF NOT EXISTS event (
                                id                INTEGER PRIMARY KEY,
                                date              INTEGER NOT NULL,
                                date_julian       REAL    NOT NULL,
                                event_description TEXT    NOT NULL
                                );
                                """
    sql_create_death_table = """ CREATE TABLE IF NOT EXISTS death (
                                id                 INTEGER PRIMARY KEY,
                                date               INTEGER NOT NULL,
                                date_julian        REAL    NOT NULL,
                                name               STRING,
                                person_description TEXT
                                );
                                """
    sql_create_birth_table = """ CREATE TABLE IF NOT EXISTS birth (
                                    id                 INTEGER PRIMARY KEY,
                                    date               INTEGER NOT NULL,
                                    date_julian        REAL    NOT NULL,
                                    name               STRING,
                                    person_description TEXT
                                    );
                                    """
    conn = create_connection()
    list_missed_days = []
    if conn is not None:
        # create tables
        create_table(conn, sql_create_events_table)
        create_table(conn, sql_create_death_table)
        create_table(conn, sql_create_birth_table)

    # create a list with all days
    days = get_all_days()

    for day in days:
        print(day)
        try:
            event, birth, death = download_from_wiki(page=day)
            insert_data_into_db(event)
            insert_data_into_db(birth)
            insert_data_into_db(death)
        except Exception as e:
            print(e)
            print("Exception occurred by date{}".format(day))
            list_missed_days.append(day)
            pass


    print(list_missed_days)
    delete_dummy(conn)

    # test for single date

    #event, birth, death = download_from_wiki(page="January_1")
    #insert_data_into_db(event)
    #insert_data_into_db(birth)
    #insert_data_into_db(death)
    print("Ready!")
