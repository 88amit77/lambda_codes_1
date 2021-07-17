import pandas as pd

import psycopg2
import sys
import csv
from datetime import date

conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
cur_users = conn_users.cursor()
QUERY='select attendance_leave_id,ar_id_id,emp_id_id,api_attendence_rules.ar_name,api_employee.name ' \
'from api_attendenceleaveid ' \
'join api_employee on api_attendenceleaveid.emp_id_id = api_employee.emp_id ' \
'join api_attendence_rules on api_attendenceleaveid.ar_id_id = api_attendence_rules.ar_id'
rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
db_name = "employees"

conn_employees = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
cur_employees = conn_employees.cursor()
# data= cur_employees.fetchone()
# print(data)
#####test for query param####
export_file_query = "Select * from api_export where file_type='Employees' and exfile_iscreated = FALSE LIMIT 1"
cur_users.execute(export_file_query)
exportt = cur_users.fetchall()
for row in exportt:
    # final=row('user_id')

    print(row[13])
    final=row[13]
print("user...table")
print(exportt)
print("final:",final)
######test for query param####

cur=conn_employees.cursor()
cur.execute(QUERY)
result=cur.fetchall()

# c = csv.writer(open('/Users/amittiwari/Downloads/csv/dbdump01.csv', 'w'))
data = [[
            'Attendance ID',
            'Emp ID',
            'Name',
            'Ar ID',
            "AR Name",

            ]]
for employee in result:
    print(employee)

    data.append([

        employee[0],
        employee[2],
        employee[4],
        employee[1],
        employee[3],

    ])
    print("data=")
    # data.append([d,])
    print(data)
    # t = (1, 2, 3)
    # data = data + (d,)
    # print("last")
    # print(data)
    # t
    # (1, 2, 3, 1)


# for x in result:
#     c.writerow(x)
# file_name = '/Users/amittiwari/Downloads/csv/dbdump01.csv'
file_from = '/Users/amittiwari/Downloads/csv/work1.csv'
with open(file_from, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)
