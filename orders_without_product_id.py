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


# def lambda_handler(event, context):
def lambda_handler():

    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                  host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_users = conn_users.cursor()

    conn_products = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur_products = conn_products.cursor()
    # Status check
    status_export_file_query = "Select * from api_export where file_type='New_Order_Without_Product_ID' and status='Generating'"
    cur_users.execute(status_export_file_query)
    status_exports = cur_users.fetchall()

    if len(status_exports) > 0:
        return {'status1': False}

    export_file_query = "Select * from api_export where file_type='New_Order_Without_Product_ID' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()

    if len(exports) == 0:
        return {'status2': False}
    for export in exports:
        try:

            query_pruducts = 'select order_id,order_item_id,portal_sku,portal_id from api_neworder ' \
                              'where product_id IS  NULL'

            cur_products.execute(query_pruducts)

            products = cur_products.fetchall()
            # print(products[0])
            if len(products) <= 0:
                return {'status3': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'Order ID',
                'Order Item ID',
                'Portal SKU',
                'Portal ID',
            ]]
            for product in products:
                # print(product)
                data.append([

                    product[0],
                    product[1],
                    product[2],
                    product[3],
                 ])
            file_name = 'New_Order_Without_Product_ID-'+datetime.now().strftime('%Y-%m-%d')+'.csv'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            file_to = '/buymore2/products/New_Order_Without_Product_ID/' + file_name
            access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
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
            access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
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