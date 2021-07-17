import os
import psycopg2
import dropbox
import csv
from datetime import datetime

rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
db_name = "employees"


def lambda_handler():
    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                  host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_users = conn_users.cursor()

    conn_employees = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur_employees = conn_employees.cursor()

    # Status check
    status_export_file_query = "Select * from api_export where file_type='EmployeesMonthlySalary' and status='Generating'"
    cur_users.execute(status_export_file_query)
    status_exports = cur_users.fetchall()
    if len(status_exports) > 0:
        return {'status1': False}

    export_file_query = "Select * from api_export where file_type='EmployeesMonthlySalary' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()
    #for query
    # query_params_export = "Select * from api_export where file_type='EmployeesMonthlySalary' and exfile_iscreated = FALSE LIMIT 1"
    # cur_users.execute(export_file_query)
    # exportt = cur_users.fetchall()
    # for row in exportt:
    #     print(row[13])
    #     final = row[13]
    #final = 'where emp_id_id in(3,2) and extract(month from month) in (1)'
    #for query
    if len(exports) == 0:
        return {'status2': False}
    for export in exports:
        # print(export[13])
        # print(str(export[13]))
        try:
            export_file_query1 = "Select * from api_export where file_type='EmployeesMonthlySalary' and exfile_iscreated = FALSE LIMIT 1"
            cur_users.execute(export_file_query1)
            exportss = cur_users.fetchall()
            for expo in exportss:
                final = expo[13]
                print("final: ", final)


            query_employees = 'select lop,no_of_days,ctc,basic,hra,conveyance_allowances,medical_allowance,month,id,cca_allowance ' \
                              ',pf_employer,pf_employee,pt,esi_employer,esi_employee,net_employee_payable,due_date,emp_id_id,deductions, ' \
                              'over_time,reimbursements,special_allowances,api_employee.name ' \
                              'from api_monthlyempsalary ' \
                              'join api_employee on api_monthlyempsalary.emp_id_id = api_employee.emp_id'

            # query_params
            query_employees = query_employees +" "+ str(final)

            cur_employees.execute(query_employees)
            print(query_employees)

            employees = cur_employees.fetchall()
            if len(employees) <= 0:
                return {'status': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'MS ID',
                'Emp ID',
                'Name',
                'Month',
                "LOP",
                "No Of Days",
                "CTC",
                "Basic",
                'HRA',
                "Conveyance Allowances",
                'Medical Allowance',
                "CCA allowance",
                'Pf Employer',
                'Pf employee',
                "Pt",
                "ESI Employer",
                "ESI Employee",
                "NET Employee Payable",
                'Due Date',
                "Deductions",
                'Over Time',
                "reimbursements",
                'Special Allowances',
            ]]
            for employee in employees:
                # print(employee)
                # need to test
                data.append([

                    employee[8],
                    employee[17],
                    employee[22],
                    employee[7],
                    employee[0],
                    employee[1],
                    employee[2],
                    employee[3],
                    employee[4],
                    employee[5],
                    employee[6],
                    employee[9],
                    employee[10],
                    employee[11],
                    employee[12],
                    employee[13],
                    employee[14],
                    employee[15],
                    employee[16],
                    employee[18],
                    employee[19],
                    employee[20],
                    employee[21],
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

print(lambda_handler())