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


def lambda_handler():
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

def lambda_handler():
        # print("event==")
        # print(event)
        # body = json.loads(event['body'])
        # print("body===")
        # print(body)
        # courier_received_date = body["courier_received_date"]
        # print(courier_received_date)
        # courier_received_date1 = "'" + courier_received_date + "'"
        # print(courier_received_date1)
        conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                      host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
        cur_users = conn_users.cursor()

        conn_orders = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        cur_orders = conn_orders.cursor()
        # Status check
        status_export_file_query = "Select * from api_export where file_type='NewOrder' and status='Generating'"
        cur_users.execute(status_export_file_query)
        status_exports = cur_users.fetchall()

        if len(status_exports) > 0:
            return {'status': False}

        export_file_query = "Select * from api_export where file_type='NewOrder' and exfile_iscreated = FALSE LIMIT 1"
        cur_users.execute(export_file_query)
        exports = cur_users.fetchall()

        if len(exports) == 0:
            return {'status': False}
        for export in exports:
            try:
                query_orders = 'select dd_id,product_id,order_id,order_item_id,order_date,dispatch_by_date,portal_id,portal_sku,qty,selling_price,mrp ' \
                                ',tax_rate,warehouse_id,region,payment_method,buymore_sku,portal_account_id, ' \
                                'api_dispatchdetails.location_latitude,api_dispatchdetails.location_longitude,api_dispatchdetails.email_id ' \
                                ',api_dispatchdetails.phone,api_dispatchdetails.status, ' \
                                'api_dispatchdetails.l_b_h_w, ' \
                                'api_dispatchdetails.picklist_id,api_dispatchdetails.is_mark_placed,api_dispatchdetails.have_invoice_file ' \
                                ',api_dispatchdetails.packing_status,api_dispatchdetails.is_dispatch ' \
                                ',api_dispatchdetails.dispatch_confirmed,api_dispatchdetails.is_shipment_create,api_dispatchdetails.awb ' \
                                ',api_dispatchdetails.courier_partner,api_dispatchdetails.shipment_id ' \
                                ',api_dispatchdetails.is_canceled,api_dispatchdetails.cancel_inward_bin,api_dispatchdetails.created_at ' \
                                ',api_dispatchdetails.update_at,api_dispatchdetails.mf_id_id,api_dispatchdetails.dd_paymentstatus, ' \
                                'api_dispatchdetails.dd_cancelledpaymentstatus,api_dispatchdetails.dd_id_id ' \
                                'From api_neworder ' \
                                'join api_dispatchdetails on api_neworder.dd_id = api_dispatchdetails.dd_id_id'
                # if (courier_received_date1):
                #
                #     que = "where courier_received_date =" + str(courier_received_date1)
                #
                #     final = query_orders + " " + que
                # else:
                #     final = query_orders
                # print(final)
                #
                # cur_orders.execute(final)
                cur_orders.execute(query_orders)

                orders = cur_orders.fetchall()
                if len(orders) <= 0:
                    return {'status': False}

                cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
                conn_users.commit()

                data = [[
                    'DD ID',
                    'Product ID',
                    'Order ID',
                    "Order Item ID",
                    "Order Date",
                    "Dispatch BY Date",
                    "Portal ID",
                    'Portal SKU',
                    "Quantity",
                    "Selling Price",
                    'MRP',
                    'Tax Rate',
                    'Warehouse ID',
                    'Region',
                    'Payment Method',
                    'Buymore SKU',
                    'Portal Account ID',
                    'Location Latitude',
                    'Location Longitude',
                    'Email ID',
                    'Phone',
                    'Status',
                    'L_B_H_W',
                    'Pick List ID',
                    'IS Mark Placed',
                    'Have Invoice File',
                    'Packing Status',
                    'IS Dispatched',
                    'Dispatch Confirmed',
                    'IS Shipment Created',
                    'AWB',
                    'Courier Partner',
                    'Shipment ID',
                    'IS Cancelled',
                    'Cancel In Inward Bin',
                    'Created At',
                    'Updated AT',
                    'MF ID',
                    'Payment status',
                    'cancelled Payment status',
                   ]]
                for order in orders:
                    print(order)
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
                        order[29],
                        order[30],
                        order[31],
                        order[32],
                        order[33],
                        order[34],
                        order[35],
                        order[36],
                        order[37],
                        order[38],
                        order[39],

                    ])
                file_name = 'exports-new_order' + '.csv'
                file_from = '/tmp/' + file_name
                with open(file_from, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerows(data)
                file_to = '/buymore2/orders/new_order' + file_name
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
