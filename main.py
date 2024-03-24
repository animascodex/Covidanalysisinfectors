from datetime import datetime
import sqlite3
from numpy import genfromtxt
from sqlalchemy import Column, Integer, Float, Date, String, Date,func,or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
conn = sqlite3.connect('TMAQ2_KONGJIAMING.db')
cur = conn.cursor()


class Total_Cases(Base):
    __tablename__ = "total_cases"
    #Declare to SQLAlchemy the Names of all the column and its attributes:
    case_id = Column(Integer, primary_key=True)
    provincial_case_id = Column(Integer)
    age = Column(String)
    sex = Column(String)
    health_region = Column(String)
    province = Column(String)
    country = Column(String)
    date_report = Column(Date)
    report_week = Column(Date)
    has_travel_history = Column(String)
    locally_acquired = Column(String)
    case_source = Column(String)
engine = create_engine('sqlite:///TMAQ2_KONGJIAMING.db')
#Drop Table at the start to allow easier Marking
Base.metadata.drop_all(engine)
#Create the database
Base.metadata.create_all(engine)

#Create the session
session = sessionmaker()
session.configure(bind=engine)
s = session()
try:
    import csv
    with open('TMAQ1.csv') as f:
        reader = csv.reader(f)
        next(reader, None)
        read_count = 0
        for row in reader:
            read_count+=1
            record = Total_Cases(**{
                'case_id' : row[0],
                'provincial_case_id' : row[1],
                'age' : row[2],
                'sex' : row[3],
                'health_region' : row[4],
                'province' : row[5],
                'country' : row[6],
                'date_report' : datetime.strptime(row[7], '%Y-%m-%d').date(),
                'report_week' : datetime.strptime(row[8], '%Y-%m-%d').date(),
                'has_travel_history' : row[9],
                'locally_acquired' : row[10],
                'case_source' : row[11]
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records

finally:
    cur.execute('SELECT COUNT(*) FROM total_cases')
    row_count = cur.fetchone()[0]
    print('Number of Entries entered into Database:{0}'.format(row_count))
    s.close() #Close the connection


from sqlalchemy.orm import sessionmaker
from sqlalchemy import cast, Date,extract
from datetime import datetime
session = sessionmaker()
session.configure(bind=engine)
s = session()
engine = create_engine('sqlite:///TMAQ2_KONGJIAMING.db')
#Count_Male = s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Male').count()
#Count_Months = s.query(Total_Cases.report_week).filter(extract('month', Total_Cases.report_week)==2).count()
# Count the Total Number of Males Infectors in the Month of Feb,Mar,April
Count_Male_Months_Feb =s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Male').filter(extract('month', Total_Cases.report_week)==2).count()
Count_Male_Months_Mar =s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Male').filter(extract('month', Total_Cases.report_week)==3).count()
Count_Male_Months_Apr =s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Male').filter(extract('month', Total_Cases.report_week)==4).count()

# Count the Total Number of Female Infectors in the Month of Feb,Mar,April
Count_Female_Months_Feb =s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Female').filter(extract('month', Total_Cases.report_week)==2).count()
Count_Female_Months_Mar =s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Female').filter(extract('month', Total_Cases.report_week)==3).count()
Count_Female_Months_Apr =s.query(Total_Cases.sex).filter(Total_Cases.sex == 'Female').filter(extract('month', Total_Cases.report_week)==4).count()
print('Total Number of Male and Female Infectors for each month:')
print('February: Males: {0}  Females: {1}'.format(Count_Male_Months_Feb,Count_Female_Months_Feb))
print('March: Males: {0}  Females: {1}'.format(Count_Male_Months_Mar,Count_Female_Months_Mar))
print('April: Males: {0}  Females: {1}'.format(Count_Male_Months_Apr,Count_Female_Months_Apr))


Count_Female = s.query(Total_Cases.sex,Total_Cases.age,func.count(Total_Cases.sex)).filter(Total_Cases.sex == 'Female').group_by(Total_Cases.age).                        order_by(Total_Cases.age.desc())
Count_Test   =s.query(Total_Cases.sex, func.count(Total_Cases.sex)).group_by(Total_Cases.age)
print("Age Group:    Number of Female Infectors")
for user,age,number in (Count_Female):
    print('    {0}               '.format(age),number)
    break

Count_Age_Feb = s.query(Total_Cases.age, Total_Cases.has_travel_history, func.count(Total_Cases.age)).filter(
    Total_Cases.has_travel_history == 'f').filter(or_(Total_Cases.age == '50-59',
                                                      Total_Cases.age == '60-69', Total_Cases.age == '70-79',
                                                      Total_Cases.age == '80-89', Total_Cases.age == '90-99')). \
    filter(extract('month', Total_Cases.report_week) == 2)

# Filter Age Groups and Extract only values with March entries
Count_Age_Mar = s.query(Total_Cases.age, Total_Cases.has_travel_history, func.count(Total_Cases.age)).filter(
    Total_Cases.has_travel_history == 'f').filter(or_(Total_Cases.age == '50-59',
                                                      Total_Cases.age == '60-69', Total_Cases.age == '70-79',
                                                      Total_Cases.age == '80-89', Total_Cases.age == '90-99')). \
    filter(extract('month', Total_Cases.report_week) == 3)

# Filter Age Groups and Extract only values with April entries
Count_Age_Apr = s.query(Total_Cases.age, Total_Cases.has_travel_history, func.count(Total_Cases.age)).filter(
    Total_Cases.has_travel_history == 'f').filter(or_(Total_Cases.age == '50-59',
                                                      Total_Cases.age == '60-69', Total_Cases.age == '70-79',
                                                      Total_Cases.age == '80-89', Total_Cases.age == '90-99')). \
    filter(extract('month', Total_Cases.report_week) == 4)

Count = s.query(Total_Cases.has_travel_history).filter(Total_Cases.has_travel_history == 'f').count()
print('Top 2 Months for Those Older than 50')
for user, age, Total_Number in (Count_Age_Apr):
    print('          April: {0}               '.format(Total_Number))
for user, age, Total_Number in (Count_Age_Mar):
    print('          March: {0}               '.format(Total_Number))
for user, age, Total_Number in (Count_Age_Feb):
    print('\nFebruary only has {0} infectors older than 50'.format(Total_Number))


