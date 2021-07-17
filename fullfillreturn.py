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
    portal_id = body["portal_id"]
    print(portal_id)
    actual_return_date = body["actual_return_date"]
    print(actual_return_date)
    actual_return_date1 = "'" + actual_return_date + "'"
    print(actual_return_date1)
    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                  host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_users = conn_users.cursor()

    conn_orders = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur_orders = conn_orders.cursor()
    # Status check
    status_export_file_query = "Select * from api_export where file_type='FullFill_Return' and status='Generating'"
    cur_users.execute(status_export_file_query)
    status_exports = cur_users.fetchall()

    if len(status_exports) > 0:
        return {'status': False}

    export_file_query = "Select * from api_export where file_type='FullFill_Return' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()

    if len(exports) == 0:
        return {'status': False}
    for export in exports:
        try:
            query_orders = 'select dd_id,portal_id,api_fulfilledreturn.fr_id,api_fulfilledreturn.return_request_date,api_fulfilledreturn.actual_return_date, ' \
                           'api_fulfilledreturn.destination_warehouse_id,api_fulfilledreturn.return_reason,api_fulfilledreturn.sub_reason,api_fulfilledreturn.awb, ' \
                           'api_fulfilledreturn.return_type,api_fulfilledreturn.dd_id_id,api_fulfilledreturn.pod_id_id, ' \
                           'api_refundimagetable.id,api_refundimagetable.image_list,api_refundimagetable.return_category,api_refundimagetable.return_notes, ' \
                           'api_refundimagetable.tracking_id,api_refundimagetable.created_at,api_refundimagetable.updated_at,api_refundimagetable.processing_date, ' \
                           'api_refundimagetable.return_type,api_refundimagetable.package_condition,api_refundimagetable.is_barcode_required,api_refundimagetable.product_condition, ' \
                           'api_refundimagetable.image_correctness,api_refundimagetable.size_correctness,api_refundimagetable.alternate_product_id,api_refundimagetable.sellable, ' \
                           'api_refundimagetable.dd_id_id,api_refundimagetable.pod_id_id ' \
                           'from api_neworder ' \
                           'join api_fulfilledreturn on api_neworder.dd_id = api_fulfilledreturn.dd_id_id ' \
                           'join api_refundimagetable on api_neworder.dd_id = api_refundimagetable.dd_id_id'
            if (portal_id):

                que = "where portal_id =" + str(portal_id) + " and api_fulfilledreturn.actual_return_date =" + str(
                    actual_return_date1)

                final = query_orders + " " + que
            else:
                final = query_orders
            print(final)

            cur_orders.execute(final)
            # cur_orders.execute(query_orders)

            orders = cur_orders.fetchall()
            if len(orders) <= 0:
                return {'status': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'DD ID',
                'Portal ID',
                'FR ID',
                'Return Request Date',
                'Actual Return Date',
                'Destination Warehouse ID',
                'Return Reason',
                'Sub Reason',
                'AWB',
                'Return Type',
                # 'dd_id_id',
                'POD ID',
                'ID',
                'Image List',
                'Return Category',
                'Return Notes',
                'Tracking ID',
                'Created AT',
                'Updated AT',
                'Processing Date',
                'Return Type',
                'Package Condition',
                'Is Barcode Required',
                'Product Required',
                'Product Condition',
                'Image Correctness',
                'Size Correctness',
                'Alternate Product ID',
                'Sellable',

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
                    # order[10],
                    order[11],
                    order[12],
                    order[13],
                    order[14],
                    order[15],
                    order[16],
                    order[17],
                    order[18],
                    order[19],
                    order[20],
                    order[21],
                    order[22],
                    order[23],
                    order[24],
                    order[25],
                    order[26],
                    order[27],
                    order[28],

                ])
            file_name = 'exports-fullfill_return-' + str(actual_return_date) + '.csv'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            file_to = '/buymore2/orders/fullfillreturn/' + file_name
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