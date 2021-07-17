import os
import psycopg2
import dropbox
import csv
from datetime import datetime

rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
db_name = "employees"


def lambda_handler(event, context):
    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                  host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_users = conn_users.cursor()

    conn_employees = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur_employees = conn_employees.cursor()

    # Status check
    status_export_file_query = "Select * from api_export where file_type='Employees' and status='Generating'"
    cur_users.execute(status_export_file_query)
    status_exports = cur_users.fetchall()
    if len(status_exports) > 0:
        return {'status': False}

    export_file_query = "Select * from api_export where file_type='Employees' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()

    if len(exports) == 0:
        return {'status': False}
    for export in exports:
        try:
            query_employees = 'SELECT api_employee.name,api_empleaveapplied.*,api_leaverules.leave_name ' \
                              'FROM api_employee, api_empleaveapplied,api_leaverules ' \
                              'WHERE api_employee.emp_id = api_empleaveapplied.emp_id_id and api_empleaveapplied.leave_id_id=api_leaverules.leave_id'


            query_employees += ' where col_name = col_val and '

            cur_employees.execute(query_employees)

            employees = cur_employees.fetchall()
            if len(employees) <= 0:
                return {'status': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'LT ID'
                'Emp ID',
                'Name',
                'Leave ID',
                "Leave Name",
                "Start Date",
                "End Date",
                "Status",
                'Reason',
                "Action By ID",

            ]]
            for employee in employees:
                print(employee)
                # need to test
                data.append([

                    employee[1],
                    employee[7],
                    employee[0],
                    employee[8],
                    employee[9],
                    employee[2],
                    employee[3],
                    employee[4],
                    employee[5],
                    employee[6],

                ])

            file_name = 'exports-' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            file_to = '/buymore2/employees/' + file_name
            access_token = 'd7ElXR2Sr-AAAAAAAAAAC2HC0qc45ss1TYhRYB4Jy6__NJU1jjGiffP7LlP_2rrf'
            dbx = dropbox.Dropbox(access_token)
            file_size = os.path.getsize(file_from)
            with open(file_from, 'rb') as f:
                dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

            cur_users.execute("Update api_export set exfile_path='" + file_to + "', exfile_iscreated=TRUE,"
                                                                                " exfile_name='" + file_name + "', exfile_size='" + str(
                file_size) + "', status='Generated' "
                             "where export_id=" + str(export[0]))
            conn_users.commit()
        except:
            file_name = 'exports_error-' + str(int(datetime.timestamp(datetime.now()))) + '.log'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                file.write('Exception occurred')
            file_to = '/buymore2/employees/logs/' + file_name
            access_token = 'd7ElXR2Sr-AAAAAAAAAAC2HC0qc45ss1TYhRYB4Jy6__NJU1jjGiffP7LlP_2rrf'
            dbx = dropbox.Dropbox(access_token)
            file_size = os.path.getsize(file_from)
            with open(file_from, 'rb') as f:
                dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

            cur_users.execute(
                "Update api_export set exfile_errorlog='" + file_to + "',  status='Failed' where export_id=" + str(
                    export[0]))
            conn_users.commit()
    return {
        'status': True
    }