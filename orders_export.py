import os
import psycopg2
import dropbox
import csv
from datetime import datetime


def lambda_handler():
    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                  host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    conn_orders = psycopg2.connect(database="orders", user="postgres", password="buymore2",
                                   host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")

    cur_users = conn_users.cursor()
    print(conn_users)
    print(conn_orders)

    cur_orders = conn_orders.cursor()
    print(cur_orders)

    # Status check
    status_export_file_query = "Select * from api_export where file_type='Orders' and status='Generating'"
    cur_users.execute(status_export_file_query)
    status_exports = cur_users.fetchall()
    if len(status_exports) > 0:
        return {'status': False}


    export_file_query = "Select * from api_export where file_type='Orders' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()
    print(exports)

    if len(exports) == 0:
        return {'status': False}
    for export in exports:

        try:

            query_orders = 'select * from api_neworder inner join api_dispatchdetails ' \
                           'on api_neworder.dd_id = api_dispatchdetails.dd_id_id inner join api_manifest ' \
                           'on api_manifest.mf_id = api_dispatchdetails.mf_id_id inner join api_fulfilledreturn ' \
                           'on api_fulfilledreturn.dd_id = api_neworder.dd_id'

            # query_params
            query_orders += ' where col_name = col_val and '

            cur_orders.execute(query_orders)

            orders = cur_orders.fetchall()

            if len(orders) <= 0:
                return {'status': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'Buymore_Order_id',
                'DD_id',
                'Product_Id',
                'order_id',
                'Order_Item_Id',
                'Order_date',
                'Dispatch_By_Date',
                'Portal_name',
                'Portal_Sku',
                'Qty',
                'Selling_price',
                'MRP',
                'Tax_Amount',
                'principle',
                'Region',
                'Payment_method',
                'Manifest date',
                'return request date',
                'return type',
                'is cancelled',
                'Order status'
            ]]
            for order in orders:
                print(order)
                data.append([
                    '',
                    order[15],
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
                    order[11],
                    '',
                    order[13],
                    order[14],
                    order[46],
                    order[48],
                    order[54],
                    order[37],
                    order[24]
                ])

            file_name = 'exports-' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            file_to = '/buymore2/orders/' + file_name
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


print(lambda_handler())