import json
import os
import psycopg2
import dropbox
import csv
from datetime import datetime

rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
db_name = "orders"


def lambda_handler(event, context):
    print("event==")
    print(event)
    body = json.loads(event['body'])
    print("body===")
    print(body)
    courier_received_date = body["courier_received_date"]
    print(courier_received_date)
    courier_received_date1 = "'" + courier_received_date + "'"
    print(courier_received_date1)
    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                  host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_users = conn_users.cursor()

    conn_orders = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur_orders = conn_orders.cursor()
    # Status check
    status_export_file_query = "Select * from api_export where file_type='PodList' and status='Generating'"
    cur_users.execute(status_export_file_query)
    status_exports = cur_users.fetchall()

    if len(status_exports) > 0:
        return {'status': False}

    export_file_query = "Select * from api_export where file_type='PodList' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()

    if len(exports) == 0:
        return {'status': False}
    for export in exports:
        try:
            query_orders = 'SELECT api_podlist.* ' \
                           'FROM api_podlist'
            if (courier_received_date1):

                que = "where courier_received_date =" + str(courier_received_date1)

                final = query_orders + " " + que
            else:
                final = query_orders
            print(final)

            cur_orders.execute(final)

            orders = cur_orders.fetchall()
            if len(orders) <= 0:
                return {'status': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'POD ID',
                'POD Number',
                'Courier Partner Name',
                "Pod Image List",
                "Total Quantity Received",
                "Processed Quantity",
                "Warehouse ID",
                'Courier Received Data',
                "Created BY",
                "Status",
                'Updated At',
            ]]
            for order in orders:
                # print(order)
                data.append([

                    order[0],
                    order[1],
                    order[2],
                    order[3],
                    order[4],
                    order[5],
                    order[6],
                    order[7],
                    order[8],
                    order[9],
                    order[10],
                ])
            file_name = 'exports-podlist' + '.csv'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            file_to = '/buymore2/orders/podlist' + file_name
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
            file_to = '/buymore2/orders/logs/' + file_name
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