import json
import os
import psycopg2
import dropbox
from datetime import date
import csv
from datetime import datetime
import datetime
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import requests



def db_credential(db_name):
    url = "http://ec2-13-234-21-229.ap-south-1.compute.amazonaws.com/db_credentials/"

    payload = json.dumps({
        "data_base_name": db_name
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = dict(requests.post(url, data=payload, headers=headers).json())
    print("response===>", response)
    status = response['status']
    print("payload", payload)
    if status == True:
        return {

            'response': response
        }
    else:
        return {
            'response': response
        }


def lambda_handler(event, context):
# def lambda_handler():
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##orders
    cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
    print("cred_for_sqlalchemy_orders--", cred_for_sqlalchemy_orders)
    ##employees
    cred_for_sqlalchemy_employees = cred_for_sqlalchemy + "/employees"
    print("cred_for_sqlalchemy_employees--", cred_for_sqlalchemy_employees)
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
    ##users
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    print("cred_for_sqlalchemy_users--", cred_for_sqlalchemy_users)
    ##vendors
    cred_for_sqlalchemy_vendors = cred_for_sqlalchemy + "/vendors"
    print("cred_for_sqlalchemy_vendors--", cred_for_sqlalchemy_vendors)
    print("event==")
    print(event)
    body = json.loads(event['body'])
    print("body===")
    print(body)
    emp_id = body["emp_id"]
    print(emp_id)
    start_date1 = body["start_date"]
    end_date1 = body["end_date"]

    # emp_id = 172
    #emp_id = 151
    ####for curent month
    # start_date1 = '2021-04-01'
    print('start_date1', start_date1)
    start_date2 = "'" + start_date1 + "'"
    print('start_date2', start_date2)

    # end_date1 = '2021-04-30'
    print('end_date1', end_date1)
    end_date2 = "'" + end_date1 + "'"
    print('end_date1', end_date2)
    ####for curent month

    ###for previous month
    start_date10 = '2021-04-01'
    print('start_date10', start_date10)
    start_date20 = "'" + start_date10 + "'"
    print('start_date20', start_date20)

    end_date10 = '2021-04-30'
    print('end_date10', end_date10)
    end_date20 = "'" + end_date10 + "'"
    print('end_date10', end_date20)

    ###for previous month


    # emp_id =180
    # start_date = '2021-02-01'
    # end_date = '2021-02-28'
    start_date = start_date2
    end_date = end_date2
    # emp_id = 219
    ###privious month data
    if emp_id:
        start_date = start_date20
        end_date = end_date20
        sdate = start_date10
        ldate = end_date10
        sdate1 = datetime.datetime.strptime(sdate, "%Y-%m-%d").date()
        print('sdate1', sdate1)
        ldate1 = datetime.datetime.strptime(ldate, "%Y-%m-%d").date()
        print('ldate1', ldate1)
        duration = (ldate1 - sdate1).days
        total_day = duration + 1
        print(total_day)
        data1 = pd.DataFrame(columns=['date', 'day_of_week'], index=range(total_day))
        ###dumy row create as per days in month
        data1.to_csv('/tmp/a0.csv', index=False)

        data2 = pd.read_csv('/tmp/a0.csv')
        ###date created and then weak days calculation and converted in to days
        data2['date'] = pd.date_range(start_date, freq='D', periods=len(data2)).strftime('%Y-%m-%d')
        # print (df)
        dr = pd.date_range(start=start_date, end=end_date)
        data3 = pd.DataFrame()
        data3['date'] = dr
        data3['day_of_week'] = pd.to_datetime(dr).weekday
        data3['day_of_week'] = data3['day_of_week'].astype(str)
        data3.loc[data3['day_of_week'].str.contains('6'), 'day_of_week'] = 'Sun'
        data3.loc[data3['day_of_week'].str.contains('0'), 'day_of_week'] = 'Mon'
        data3.loc[data3['day_of_week'].str.contains('1'), 'day_of_week'] = 'Tue'
        data3.loc[data3['day_of_week'].str.contains('2'), 'day_of_week'] = 'Wed'
        data3.loc[data3['day_of_week'].str.contains('3'), 'day_of_week'] = 'Thur'
        data3.loc[data3['day_of_week'].str.contains('4'), 'day_of_week'] = 'Fri'
        data3.loc[data3['day_of_week'].str.contains('5'), 'day_of_week'] = 'Sat'
        data3['work_date'] = data3['date']

        data3.to_csv('/tmp/a1.csv', index=False)
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
        query = 'SELECT api_employee.name,attendance_id,work_date,emp_id_id,work_hrs ,lop ' \
                'FROM api_attendance,api_employee ' \
                'WHERE api_attendance.emp_id_id = api_employee.emp_id and emp_id_id = ' + str(emp_id)
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/a2.csv', index=False)
        df11 = pd.read_csv("/tmp/a1.csv")
        df12 = pd.read_csv("/tmp/a2.csv")
        df22 = pd.merge(left=df12, right=df11, left_on='work_date', right_on='work_date')

        ###present data in a month
        df22.to_csv('/tmp/a3.csv', index=False)
        # df22.to_csv('/tmp/3.csv', index=False)
        # test = pd.DataFrame(columns=['name','attendance_id','work_date','emp_id_id','work_hrs','date','day_of_week'], index=range(total_day))
        test = pd.read_csv('/tmp/a1.csv')
        test['name'] = 'NaN'
        test['attendance_id'] = 'NaN'
        test['emp_id_id'] = 'NaN'
        test['work_hrs'] = 'NaN'
        test['lop'] = 'NaN'

        test.to_csv('/tmp/a4.csv', index=False)
        ###for ordering
        test1 = test[["name", "attendance_id", "work_date", "emp_id_id", "work_hrs", "lop", "date",
                      "day_of_week"]]
        ##full month data
        test1.to_csv('/tmp/a5.csv', index=False)
        cc = list(df22['work_date'])
        dd = test1[~test1['work_date'].isin(cc)]
        # dd['name']=df22['name']
        dd.to_csv('/tmp/a6.csv', index=False)
        dd.to_csv('/tmp/a3.csv', mode='a', index=False, header=False)
        attand = pd.read_csv('/tmp/a3.csv')
        data = pd.read_csv('/tmp/a3.csv')
        emp_name = data['name'].iloc[0]
        print("name", emp_name)
        attendance_ids = data['attendance_id'].iloc[0]
        print("attendance_ids", attendance_ids)
        emp_id_ids = data['emp_id_id'].iloc[0]
        print("emp_id_ids", emp_id_ids)
        ###to make data in in order
        attand.sort_values(by=['work_date'], inplace=True, ascending=True)
        ##to fill all data with same emp data
        attand.name.fillna(emp_name, inplace=True)
        attand.attendance_id.fillna(attendance_ids, inplace=True)
        attand.emp_id_id.fillna(emp_id_ids, inplace=True)

        attand.fillna(0, inplace=True, downcast='infer')
        attand['attandance'] = 0
        attand['attandance'] = np.where(attand['work_hrs'] <= 0, '0', '1')
        print(attand['attandance'].dtype)
        attand['attandance'] = attand['attandance'].astype(float)
        print(attand['attandance'].dtype)
        print(attand['lop'].dtype)
        ##diff between attendance and lop column
        attand['final_attandance'] = attand['attandance'] - attand['lop']
        attand.to_csv('/tmp/a7.csv', index=False)

        ###till attendance monthly
        ##now for approved leaves
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
        query1 = "select * from api_empleaveapplied where (start_date >= " + "'" + str(
            start_date10) + "'" + " and end_date <= " + "'" + str(
            end_date10) + "'" + "and status = 'APPROVED') and emp_id_id = " + str(emp_id)

        sql1 = pd.read_sql(query1, engine)
        sql1.to_csv('/tmp/a8.csv', index=False)
        df = pd.read_csv('/tmp/a8.csv')

        ###logic to fins all leave approved date range

        ####condition  if leave approved is empty
        if df.empty:
            leave_with_no_data = pd.read_csv('/tmp/a7.csv',
                                             usecols=['name', 'attendance_id', 'work_date', 'emp_id_id', 'work_hrs',
                                                      'lop',
                                                      'day_of_week', 'attandance', 'final_attandance'],
                                             low_memory=False)
            leave_with_no_data['aproved_leave'] = 0
            ##for proper ordering
            leave_with_no_data_1 = leave_with_no_data[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs', 'lop',
                 'attandance', 'final_attandance', 'aproved_leave']]
            leave_with_no_data_1.to_csv('/tmp/a12.csv', index=False)
        else:

            b = pd.concat([pd.DataFrame({'start_date': pd.date_range(row.start_date, row.end_date, freq='d'),
                                         }, columns=['start_date'])
                           for i, row in df.iterrows()], ignore_index=True)

            # b=pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
            # print("b",b)
            b.rename(columns={'start_date': 'date'}, inplace=True)
            b['aproved_leave'] = 1
            b.to_csv('/tmp/a9.csv', index=False)
            df11 = pd.read_csv("/tmp/a1.csv")
            df12 = pd.read_csv("/tmp/a9.csv")
            ###left join to merge between total month and approved leave days data
            df22 = pd.merge(df11, df12, how='left', on=['date'])
            df22['aproved_leave'] = df22['aproved_leave'].fillna(0)
            df22.to_csv('/tmp/a10.csv')
            ##now merge all data till approved leave
            df111 = pd.read_csv("/tmp/a7.csv")
            df121 = pd.read_csv("/tmp/a10.csv")
            df221 = pd.merge(left=df121, right=df111, left_on='work_date', right_on='work_date')
            # df22 = pd.concat([df12, df11])
            ###present data in a month
            df221.rename(columns={'date_x': 'date'}, inplace=True)
            df221.rename(columns={'day_of_week_x': 'day_of_week'}, inplace=True)
            df221.to_csv('/tmp/a11.csv', index=False)
            leave_approved = pd.read_csv('/tmp/a11.csv',
                                         usecols=['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week',
                                                  'work_hrs',
                                                  'lop', 'attandance', 'final_attandance', 'aproved_leave'],
                                         low_memory=False)
            ##for proper ordering
            leave_approved_1 = leave_approved[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs',
                 'lop', 'attandance', 'final_attandance', 'aproved_leave']]
            leave_approved_1.to_csv('/tmp/a12.csv', index=False)

        ##till leave approved
        ###attandance rule(weakly off,random,sunday)
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
        query3 = 'select attendance_leave_id,ar_id_id,emp_id_id,api_attendence_rules.ar_name,api_attendence_rules.random_weekly_off,api_attendence_rules.sunday_off,api_attendence_rules.saturday_sunday_off,api_employee.name ' \
                 'from api_attendenceleaveid ' \
                 'join api_employee on api_attendenceleaveid.emp_id_id = api_employee.emp_id ' \
                 'join api_attendence_rules on api_attendenceleaveid.ar_id_id = api_attendence_rules.ar_id ' \
                 'where emp_id_id = ' + str(emp_id)
        sql2 = pd.read_sql(query3, engine)
        sql2.to_csv('/tmp/a14.csv', index=False)
        weakly_data = pd.read_csv('/tmp/a14.csv',
                                  usecols=['emp_id_id', 'ar_name', 'random_weekly_off',
                                           'sunday_off',
                                           'saturday_sunday_off'],
                                  low_memory=False)
        ##for proper ordering

        weakly_data_1 = weakly_data[['emp_id_id', 'ar_name', 'random_weekly_off',
                                     'sunday_off',
                                     'saturday_sunday_off']]
        weakly_data_1.to_csv('/tmp/a15.csv', index=False)
        df1111 = pd.read_csv("/tmp/a12.csv")
        df1222 = pd.read_csv("/tmp/a15.csv")
        ###left join to merge between total month and approved leave days data
        df2222 = pd.merge(df1111, df1222, how='left', on=['emp_id_id'])
        df2222['final_random_weekly_off'] = 0
        df2222['final_sunday_off'] = 0
        df2222['final_saturday_sunday_off'] = 0
        data3333 = df2222.drop_duplicates(subset="work_date", keep='last')
        data3333.to_csv('/tmp/a16.csv', index=False)
        # df2222.to_csv('/tmp/a16.csv', index=False)
        # data manupulation work for off days in a weak(sunday off and sun+sat off)
        dfread = pd.read_csv("/tmp/a16.csv")

        dfread['final_saturday_sunday_off'] = np.where(
            (dfread['saturday_sunday_off'] == True) & (dfread['day_of_week'] == 'Sat'), 1,
            np.where((dfread['saturday_sunday_off'] == 1) & (dfread['day_of_week'] == 'Sun'), 1, 0))
        dfread['final_sunday_off'] = np.where((dfread['sunday_off'] == True) & (dfread['day_of_week'] == 'Sun'), 1, 0)
        ###if aprrove leave is there then updated that in attandanc
        dfread['attandance'] = np.where(dfread["aproved_leave"] == 1, dfread['aproved_leave'], dfread['attandance'])

        dfread.to_csv('/tmp/a17.csv', index=False)

        ##data manupulation work for off days in a weak(random weekly off)
        # work_date,day_of_week

        dddd = pd.read_csv('/tmp/a17.csv',
                           usecols=['random_weekly_off', 'day_of_week', 'work_date', 'attandance',
                                    'final_random_weekly_off'],
                           low_memory=False)
        dddd.to_csv('/tmp/a27.csv', index=False)
        df1 = pd.read_csv("/tmp/a27.csv")
        ##for creating week no
        df1['work_date'] = pd.to_datetime(df1['work_date'], errors='coerce')
        df1['Week_Number'] = df1['work_date'].dt.week
        #
        df1.to_csv('/tmp/a30.csv', index=False)

        Week_Number = df1['Week_Number'].to_list()
        ##to remove duplicates
        Week_Number1 = list(set(Week_Number))
        print('Week_Number1', Week_Number1)

        data_random = df1.loc[df1.random_weekly_off == True]
        print('data_random 258 line--', data_random)
        ###in case of random day off this code run else will take other one
        if data_random.empty:
            ###for sunday of data and sat-sunday of data
            fixed_off = pd.read_csv('/tmp/a17.csv')
            fixed_off1 = fixed_off.drop(['random_weekly_off', 'saturday_sunday_off', 'sunday_off'], axis=1)
            fixed_off1.to_csv('/tmp/b1.csv', index=False)
            fixed_off2 = pd.read_csv('/tmp/b1.csv')
            fixed_off2.rename(columns={'final_attandance': 'attandance(attandance-lop)'}, inplace=True)
            fixed_off2.rename(columns={'final_random_weekly_off': 'random_weekly_off'}, inplace=True)
            fixed_off2.rename(columns={'final_saturday_sunday_off': 'saturday_sunday_off'}, inplace=True)
            fixed_off2.rename(columns={'final_sunday_off': 'sunday_off'}, inplace=True)
            fixed_off2['final_cal'] = fixed_off2['attandance'] + fixed_off2['saturday_sunday_off'] + \
                                      fixed_off2['sunday_off'] + fixed_off2['aproved_leave'] + fixed_off2[
                                          'random_weekly_off']
            fixed_off2['final'] = np.where(fixed_off2['final_cal'] == 0, 0, 1)
            fixed_off4 = fixed_off2.drop(['final_cal'], axis=1)
            fixed_off4.rename(columns={'final': 'resultant_data'}, inplace=True)
            fixed_off4.to_csv('/tmp/b2.csv', index=False)
            fixed_off5 = pd.read_csv('/tmp/b2.csv')

            fixed_off5 = fixed_off5[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs', 'lop', 'ar_name',
                 'attandance', 'attandance(attandance-lop)', 'aproved_leave'
                    , 'random_weekly_off', 'saturday_sunday_off', 'sunday_off', 'resultant_data']]

            # fixed_off5.to_csv('aa26.csv', index=False)
            fixed_off5.to_csv('/tmp/aab26.csv', index=False)
            data_final_final1 = pd.read_csv("/tmp/aab26.csv")
            data_final_final1['resultant_data'] = data_final_final1['resultant_data'] - data_final_final1['lop']
            data_final_final1.to_csv('/tmp/aa26.csv', index=False)
            # emp_details.to_csv('aa26.csv', index=False)
            # engine.dispose()
            cc1 = pd.read_csv("/tmp/aa26.csv")
            print("cc1", cc1)
            print("cc1", cc1)
            ###for dynamic end date cal
            # end_date2 ='2021-03-30'
            print(end_date20)
            end_date4 = end_date20[:-3]
            print(end_date4)
            last_date = '25'
            final_date0 = end_date4 + last_date
            print('final_date0---', final_date0)
            final_date1 = final_date0.replace("'", "")
            # print("cc1", cc1)
            data_data1 = cc1[cc1['work_date'] > final_date1]
            ###code commented
            # data_data1 = cc1[cc1['work_date'] < '2021-03-26']
            data_data1.to_csv('/tmp/z1999xx.csv', index=False)
            cc4455xx = pd.read_csv("/tmp/z1999xx.csv")
            print("cc44xx", cc4455xx)
        else:
            ##create 5 empty csv file with headers
            df1.to_csv('/tmp/a32.csv', index=False)
            # df1.to_csv('a32.csv', index=False)
            dff = pd.read_csv("/tmp/a27.csv")
            dfff = dff.iloc[0:0]
            # dfff.to_csv('a300_test.csv', index=False)
            dfff.to_csv('/tmp/a300.csv', index=False)
            dfff.to_csv('/tmp/a310.csv', index=False)
            dfff.to_csv('/tmp/a311.csv', index=False)
            dfff.to_csv('/tmp/a312.csv', index=False)
            dfff.to_csv('/tmp/a313.csv', index=False)
            dfff.to_csv('/tmp/a314.csv', index=False)
            dfff.to_csv('/tmp/a320.csv', index=False)
            dfff.to_csv('/tmp/a321.csv', index=False)
            dfff.to_csv('/tmp/a322.csv', index=False)
            dfff.to_csv('/tmp/a323.csv', index=False)
            dfff.to_csv('/tmp/a324.csv', index=False)
            ##end create 5 empty csv file with headers
            print("data_random.empty====>", data_random.empty)
            if data_random.empty:
                pass
            else:
                a = 0
                for i in Week_Number1:
                    d = df1[['random_weekly_off', 'day_of_week', 'work_date', 'Week_Number', 'attandance',
                             'final_random_weekly_off']][df1['Week_Number'] == i]

                    d.to_csv('/tmp/a31' + str(a) + '.csv', index=False)
                    a = a + 1

            ##week one
            if os.stat('/tmp/a310.csv').st_size == 0:
                pass
            else:
                week1 = pd.read_csv("/tmp/a310.csv")
                data1 = week1[week1['attandance'] == 0]
                print(data1)
                if data1.empty:
                    print("true")
                    week1.to_csv('/tmp/a320.csv', index=False)
                else:
                    for ind in week1.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week1['attandance'][ind] == 0:
                            # print("if true")
                            week1['final_random_weekly_off'][ind] = 1
                            week1.to_csv('/tmp/a320.csv', index=False)
                            break

            ##week two
            if os.stat('/tmp/a311.csv').st_size == 0:
                pass
            else:
                week2 = pd.read_csv("/tmp/a311.csv")
                data2 = week2[week2['attandance'] == 0]
                print(data2)
                if data2.empty:
                    print("true")
                    week2.to_csv('/tmp/a321.csv', index=False)
                else:
                    for ind in week2.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week2['attandance'][ind] == 0:
                            # print("if true")
                            week2['final_random_weekly_off'][ind] = 1
                            week2.to_csv('/tmp/a321.csv', index=False)
                            break

            ##week three
            if os.stat('/tmp/a312.csv').st_size == 0:
                pass
            else:
                week3 = pd.read_csv("/tmp/a312.csv")
                data3 = week3[week3['attandance'] == 0]
                print(data3)
                if data3.empty:
                    print("true")
                    week3.to_csv('/tmp/a322.csv', index=False)
                else:
                    for ind in week3.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week3['attandance'][ind] == 0:
                            # print("if true")
                            week3['final_random_weekly_off'][ind] = 1
                            week3.to_csv('/tmp/a322.csv', index=False)
                            break

            ##week four
            if os.stat('/tmp/a313.csv').st_size == 0:
                pass
            else:
                week4 = pd.read_csv("/tmp/a313.csv")
                data4 = week4[week4['attandance'] == 0]
                print(data4)
                if data4.empty:
                    print("true")
                    week4.to_csv('/tmp/a323.csv', index=False)
                else:
                    for ind in week4.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week4['attandance'][ind] == 0:
                            # print("if true")
                            week4['final_random_weekly_off'][ind] = 1
                            week4.to_csv('/tmp/a323.csv', index=False)
                            break

            ##week five
            if os.stat('/tmp/a314.csv').st_size == 0:
                pass
            else:
                week5 = pd.read_csv("/tmp/a314.csv")
                data5 = week5[week5['attandance'] == 0]
                print(data5)
                if data5.empty:
                    print("true")
                    week5.to_csv('/tmp/a324.csv', index=False)
                else:
                    data = (week5['attandance'] == 0).all()

                    print('data', data)
                    if data == False:
                        week5.to_csv('/tmp/a324.csv', index=False)
                    else:
                        week5 = pd.read_csv("/tmp/a314.csv")
                        print('week5------', week5)
                        for ind in week5.index:
                            # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                            if week5['attandance'][ind] == 0:
                                # print("if true")
                                week5['final_random_weekly_off'][ind] = 1
                                week5.to_csv('/tmp/a324.csv', index=False)
                                break

            ##appanding all split file in one file

            ddff = pd.read_csv("/tmp/a320.csv")
            ddff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddfff = pd.read_csv("/tmp/a321.csv")
            ddfff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddffff = pd.read_csv("/tmp/a322.csv")
            ddffff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddfffff = pd.read_csv("/tmp/a323.csv")
            ddfffff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddffffff = pd.read_csv("/tmp/a324.csv")
            ddffffff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ###final cal culation

            df555 = pd.read_csv('/tmp/a300.csv')
            ##for proper ordering

            df555.to_csv('/tmp/a301.csv', index=False)
            # df555.to_csv('a301.csv', index=False)

            ###updating random weeekly calculated data
            df115 = pd.read_csv("/tmp/a17.csv")
            df125 = pd.read_csv("/tmp/a301.csv")
            df225 = pd.merge(left=df115, right=df125, left_on='work_date', right_on='day_of_week')
            df225.to_csv('/tmp/aaa25.csv', index=False)

            df_final_cal = pd.read_csv("/tmp/aaa25.csv")

            df_final_cal.rename(columns={'final_random_weekly_off_y': 'final_random_weekly_off'}, inplace=True)

            df_final_cal.rename(columns={'final_random_weekly_off_y': 'final_random_weekly_off'}, inplace=True)
            ###test

            df_final_cal.to_csv('/tmp/aaa26.csv', index=False)
            # print("df_final_cal----",df_final_cal)
            df_final_cal.rename(columns={'attandance_x': 'attandance'}, inplace=True)
            df_final_cal.rename(columns={'work_date_x': 'work_date'}, inplace=True)
            df_final_cal.rename(columns={'day_of_week_x': 'day_of_week'}, inplace=True)
            ###weekly of field have to add
            df_final_cal['final_cal'] = df_final_cal['attandance'] + df_final_cal['final_saturday_sunday_off'] + \
                                        df_final_cal['final_sunday_off'] + df_final_cal['aproved_leave'] + df_final_cal[
                                            'final_random_weekly_off']
            df_final_cal['final'] = 0
            df_final_cal['final'] = np.where(df_final_cal['final_cal'] == 0, 0, 1)
            df_final_cal.to_csv('/tmp/a25.csv', index=False)

            ###final_csv file per employee per month
            emp_details = pd.read_csv('/tmp/a25.csv',
                                      usecols=['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week',
                                               'work_hrs',
                                               'lop',
                                               'attandance', 'final_attandance', 'aproved_leave', 'ar_name'
                                          , 'final_random_weekly_off', 'final_sunday_off', 'final_saturday_sunday_off',
                                               'final'], low_memory=False)
            emp_details = emp_details[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs', 'lop', 'ar_name',
                 'attandance', 'final_attandance', 'aproved_leave'
                    , 'final_random_weekly_off', 'final_saturday_sunday_off', 'final_sunday_off', 'final']]

            emp_details.rename(columns={'final_attandance': 'attandance(attandance-lop)'}, inplace=True)
            emp_details.rename(columns={'final_random_weekly_off': 'random_weekly_off'}, inplace=True)
            emp_details.rename(columns={'final_saturday_sunday_off': 'saturday_sunday_off'}, inplace=True)
            emp_details.rename(columns={'final_sunday_off': 'sunday_off'}, inplace=True)
            emp_details.rename(columns={'final': 'resultant_data'}, inplace=True)
            emp_details.to_csv('/tmp/aab26.csv', index=False)
            data_final_final = pd.read_csv("/tmp/aab26.csv")
            data_final_final['resultant_data'] = data_final_final['resultant_data'] - data_final_final['lop']
            data_final_final.to_csv('/tmp/aa26.csv', index=False)
            # emp_details.to_csv('aa26.csv', index=False)
            # engine.dispose()
            cc1 = pd.read_csv("/tmp/aa26.csv")
            ###for dynamic end date cal
            # end_date2 ='2021-03-30'
            print(end_date20)
            end_date4 = end_date20[:-3]
            print(end_date4)
            last_date = '25'
            final_date0 = end_date4 + last_date
            print('final_date0---', final_date0)
            final_date1 = final_date0.replace("'", "")
            # print("cc1", cc1)
            data_data1 = cc1[cc1['work_date'] > final_date1]
            ###code commented
            # data_data1 = cc1[cc1['work_date'] < '2021-03-26']
            data_data1.to_csv('/tmp/z1999xx.csv', index=False)
            cc4455xx = pd.read_csv("/tmp/z1999xx.csv")
            print("cc44", cc4455xx)
            ###privious month data

    ###current month data
    if emp_id:
        start_date = start_date2
        end_date = end_date2
        sdate = start_date1
        ldate = end_date1
        sdate1 = datetime.datetime.strptime(sdate, "%Y-%m-%d").date()
        print('sdate1', sdate1)
        ldate1 = datetime.datetime.strptime(ldate, "%Y-%m-%d").date()
        print('ldate1', ldate1)
        duration = (ldate1 - sdate1).days
        total_day = duration + 1
        print(total_day)
        data1 = pd.DataFrame(columns=['date', 'day_of_week'], index=range(total_day))
        ###dumy row create as per days in month
        data1.to_csv('/tmp/a0.csv', index=False)

        data2 = pd.read_csv('/tmp/a0.csv')
        ###date created and then weak days calculation and converted in to days
        data2['date'] = pd.date_range(start_date, freq='D', periods=len(data2)).strftime('%Y-%m-%d')
        # print (df)
        dr = pd.date_range(start=start_date, end=end_date)
        data3 = pd.DataFrame()
        data3['date'] = dr
        data3['day_of_week'] = pd.to_datetime(dr).weekday
        data3['day_of_week'] = data3['day_of_week'].astype(str)
        data3.loc[data3['day_of_week'].str.contains('6'), 'day_of_week'] = 'Sun'
        data3.loc[data3['day_of_week'].str.contains('0'), 'day_of_week'] = 'Mon'
        data3.loc[data3['day_of_week'].str.contains('1'), 'day_of_week'] = 'Tue'
        data3.loc[data3['day_of_week'].str.contains('2'), 'day_of_week'] = 'Wed'
        data3.loc[data3['day_of_week'].str.contains('3'), 'day_of_week'] = 'Thur'
        data3.loc[data3['day_of_week'].str.contains('4'), 'day_of_week'] = 'Fri'
        data3.loc[data3['day_of_week'].str.contains('5'), 'day_of_week'] = 'Sat'
        data3['work_date'] = data3['date']

        data3.to_csv('/tmp/a1.csv', index=False)
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
        query = 'SELECT api_employee.name,attendance_id,work_date,emp_id_id,work_hrs ,lop ' \
                'FROM api_attendance,api_employee ' \
                'WHERE api_attendance.emp_id_id = api_employee.emp_id and emp_id_id = ' + str(emp_id)
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/a2.csv', index=False)
        df11 = pd.read_csv("/tmp/a1.csv")
        df12 = pd.read_csv("/tmp/a2.csv")
        df22 = pd.merge(left=df12, right=df11, left_on='work_date', right_on='work_date')

        ###present data in a month
        df22.to_csv('/tmp/a3.csv', index=False)
        # df22.to_csv('/tmp/3.csv', index=False)
        # test = pd.DataFrame(columns=['name','attendance_id','work_date','emp_id_id','work_hrs','date','day_of_week'], index=range(total_day))
        test = pd.read_csv('/tmp/a1.csv')
        test['name'] = 'NaN'
        test['attendance_id'] = 'NaN'
        test['emp_id_id'] = 'NaN'
        test['work_hrs'] = 'NaN'
        test['lop'] = 'NaN'

        test.to_csv('/tmp/a4.csv', index=False)
        ###for ordering
        test1 = test[["name", "attendance_id", "work_date", "emp_id_id", "work_hrs", "lop", "date",
                      "day_of_week"]]
        ##full month data
        test1.to_csv('/tmp/a5.csv', index=False)
        cc = list(df22['work_date'])
        dd = test1[~test1['work_date'].isin(cc)]
        # dd['name']=df22['name']
        dd.to_csv('/tmp/a6.csv', index=False)
        dd.to_csv('/tmp/a3.csv', mode='a', index=False, header=False)
        attand = pd.read_csv('/tmp/a3.csv')
        data = pd.read_csv('/tmp/a3.csv')
        emp_name = data['name'].iloc[0]
        print("name", emp_name)
        attendance_ids = data['attendance_id'].iloc[0]
        print("attendance_ids", attendance_ids)
        emp_id_ids = data['emp_id_id'].iloc[0]
        print("emp_id_ids", emp_id_ids)
        ###to make data in in order
        attand.sort_values(by=['work_date'], inplace=True, ascending=True)
        ##to fill all data with same emp data
        attand.name.fillna(emp_name, inplace=True)
        attand.attendance_id.fillna(attendance_ids, inplace=True)
        attand.emp_id_id.fillna(emp_id_ids, inplace=True)

        attand.fillna(0, inplace=True, downcast='infer')
        attand['attandance'] = 0
        attand['attandance'] = np.where(attand['work_hrs'] <= 0, '0', '1')
        print(attand['attandance'].dtype)
        attand['attandance'] = attand['attandance'].astype(float)
        print(attand['attandance'].dtype)
        print(attand['lop'].dtype)
        ##diff between attendance and lop column
        attand['final_attandance'] = attand['attandance'] - attand['lop']
        attand.to_csv('/tmp/a7.csv', index=False)

        ###till attendance monthly
        ##now for approved leaves
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
        query1 = "select * from api_empleaveapplied where (start_date >= " + "'" + str(
            start_date1) + "'" + " and end_date <= " + "'" + str(
            end_date1) + "'" + "and status = 'APPROVED') and emp_id_id = " + str(emp_id)

        sql1 = pd.read_sql(query1, engine)
        sql1.to_csv('/tmp/a8.csv', index=False)
        df = pd.read_csv('/tmp/a8.csv')

        ###logic to fins all leave approved date range

        ####condition  if leave approved is empty
        if df.empty:
            leave_with_no_data = pd.read_csv('/tmp/a7.csv',
                                             usecols=['name', 'attendance_id', 'work_date', 'emp_id_id', 'work_hrs', 'lop',
                                                      'day_of_week', 'attandance', 'final_attandance'],
                                             low_memory=False)
            leave_with_no_data['aproved_leave'] = 0
            ##for proper ordering
            leave_with_no_data_1 = leave_with_no_data[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs', 'lop',
                 'attandance', 'final_attandance', 'aproved_leave']]
            leave_with_no_data_1.to_csv('/tmp/a12.csv', index=False)
        else:

            b = pd.concat([pd.DataFrame({'start_date': pd.date_range(row.start_date, row.end_date, freq='d'),
                                         }, columns=['start_date'])
                           for i, row in df.iterrows()], ignore_index=True)

            # b=pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
            # print("b",b)
            b.rename(columns={'start_date': 'date'}, inplace=True)
            b['aproved_leave'] = 1
            b.to_csv('/tmp/a9.csv', index=False)
            df11 = pd.read_csv("/tmp/a1.csv")
            df12 = pd.read_csv("/tmp/a9.csv")
            ###left join to merge between total month and approved leave days data
            df22 = pd.merge(df11, df12, how='left', on=['date'])
            df22['aproved_leave'] = df22['aproved_leave'].fillna(0)
            df22.to_csv('/tmp/a10.csv')
            ##now merge all data till approved leave
            df111 = pd.read_csv("/tmp/a7.csv")
            df121 = pd.read_csv("/tmp/a10.csv")
            df221 = pd.merge(left=df121, right=df111, left_on='work_date', right_on='work_date')
            # df22 = pd.concat([df12, df11])
            ###present data in a month
            df221.rename(columns={'date_x': 'date'}, inplace=True)
            df221.rename(columns={'day_of_week_x': 'day_of_week'}, inplace=True)
            df221.to_csv('/tmp/a11.csv', index=False)
            leave_approved = pd.read_csv('/tmp/a11.csv',
                                         usecols=['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week',
                                                  'work_hrs',
                                                  'lop', 'attandance', 'final_attandance', 'aproved_leave'],
                                         low_memory=False)
            ##for proper ordering
            leave_approved_1 = leave_approved[['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs',
                                               'lop', 'attandance', 'final_attandance', 'aproved_leave']]
            leave_approved_1.to_csv('/tmp/a12.csv', index=False)

        ##till leave approved
        ###attandance rule(weakly off,random,sunday)
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
        query3 = 'select attendance_leave_id,ar_id_id,emp_id_id,api_attendence_rules.ar_name,api_attendence_rules.random_weekly_off,api_attendence_rules.sunday_off,api_attendence_rules.saturday_sunday_off,api_employee.name ' \
                 'from api_attendenceleaveid ' \
                 'join api_employee on api_attendenceleaveid.emp_id_id = api_employee.emp_id ' \
                 'join api_attendence_rules on api_attendenceleaveid.ar_id_id = api_attendence_rules.ar_id ' \
                 'where emp_id_id = ' + str(emp_id)
        sql2 = pd.read_sql(query3, engine)
        sql2.to_csv('/tmp/a14.csv', index=False)
        weakly_data = pd.read_csv('/tmp/a14.csv',
                                  usecols=['emp_id_id', 'ar_name', 'random_weekly_off',
                                           'sunday_off',
                                           'saturday_sunday_off'],
                                  low_memory=False)
        ##for proper ordering

        weakly_data_1 = weakly_data[['emp_id_id', 'ar_name', 'random_weekly_off',
                                     'sunday_off',
                                     'saturday_sunday_off']]
        weakly_data_1.to_csv('/tmp/a15.csv', index=False)
        df1111 = pd.read_csv("/tmp/a12.csv")
        df1222 = pd.read_csv("/tmp/a15.csv")
        ###left join to merge between total month and approved leave days data
        df2222 = pd.merge(df1111, df1222, how='left', on=['emp_id_id'])
        df2222['final_random_weekly_off'] = 0
        df2222['final_sunday_off'] = 0
        df2222['final_saturday_sunday_off'] = 0
        data3333 = df2222.drop_duplicates(subset="work_date", keep='last')
        data3333.to_csv('/tmp/a16.csv', index=False)
        # df2222.to_csv('/tmp/a16.csv', index=False)
        # data manupulation work for off days in a weak(sunday off and sun+sat off)
        dfread = pd.read_csv("/tmp/a16.csv")

        dfread['final_saturday_sunday_off'] = np.where(
            (dfread['saturday_sunday_off'] == True) & (dfread['day_of_week'] == 'Sat'), 1,
            np.where((dfread['saturday_sunday_off'] == 1) & (dfread['day_of_week'] == 'Sun'), 1, 0))
        dfread['final_sunday_off'] = np.where((dfread['sunday_off'] == True) & (dfread['day_of_week'] == 'Sun'), 1, 0)
        ###if aprrove leave is there then updated that in attandanc
        dfread['attandance'] = np.where(dfread["aproved_leave"] == 1, dfread['aproved_leave'], dfread['attandance'])

        dfread.to_csv('/tmp/a17.csv', index=False)

        ##data manupulation work for off days in a weak(random weekly off)
        # work_date,day_of_week

        dddd = pd.read_csv('/tmp/a17.csv',
                           usecols=['random_weekly_off', 'day_of_week', 'work_date', 'attandance',
                                    'final_random_weekly_off'],
                           low_memory=False)
        dddd.to_csv('/tmp/a27.csv', index=False)
        df1 = pd.read_csv("/tmp/a27.csv")
        ##for creating week no
        df1['work_date'] = pd.to_datetime(df1['work_date'], errors='coerce')
        df1['Week_Number'] = df1['work_date'].dt.week
        #
        df1.to_csv('/tmp/a30.csv', index=False)

        Week_Number = df1['Week_Number'].to_list()
        ##to remove duplicates
        Week_Number1 = list(set(Week_Number))
        print('Week_Number1', Week_Number1)

        data_random = df1.loc[df1.random_weekly_off == True]
        print('data_random 258 line--', data_random)
        ###in case of random day off this code run else will take other one
        if data_random.empty:
            ###for sunday of data and sat-sunday of data
            fixed_off = pd.read_csv('/tmp/a17.csv')
            fixed_off1 = fixed_off.drop(['random_weekly_off', 'saturday_sunday_off', 'sunday_off'], axis=1)
            fixed_off1.to_csv('/tmp/b1.csv', index=False)
            fixed_off2 = pd.read_csv('/tmp/b1.csv')
            fixed_off2.rename(columns={'final_attandance': 'attandance(attandance-lop)'}, inplace=True)
            fixed_off2.rename(columns={'final_random_weekly_off': 'random_weekly_off'}, inplace=True)
            fixed_off2.rename(columns={'final_saturday_sunday_off': 'saturday_sunday_off'}, inplace=True)
            fixed_off2.rename(columns={'final_sunday_off': 'sunday_off'}, inplace=True)
            fixed_off2['final_cal'] = fixed_off2['attandance'] + fixed_off2['saturday_sunday_off'] + \
                                      fixed_off2['sunday_off'] + fixed_off2['aproved_leave'] + fixed_off2[
                                          'random_weekly_off']
            fixed_off2['final'] = np.where(fixed_off2['final_cal'] == 0, 0, 1)
            fixed_off4 = fixed_off2.drop(['final_cal'], axis=1)
            fixed_off4.rename(columns={'final': 'resultant_data'}, inplace=True)
            fixed_off4.to_csv('/tmp/b2.csv', index=False)
            fixed_off5 = pd.read_csv('/tmp/b2.csv')

            fixed_off5 = fixed_off5[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs', 'lop', 'ar_name',
                 'attandance', 'attandance(attandance-lop)', 'aproved_leave'
                    , 'random_weekly_off', 'saturday_sunday_off', 'sunday_off', 'resultant_data']]

            # fixed_off5.to_csv('aa26.csv', index=False)
            fixed_off5.to_csv('/tmp/aab26.csv', index=False)
            data_final_final1 = pd.read_csv("/tmp/aab26.csv")
            data_final_final1['resultant_data'] = data_final_final1['resultant_data'] - data_final_final1['lop']
            data_final_final1.to_csv('/tmp/aa26.csv', index=False)
            # emp_details.to_csv('aa26.csv', index=False)
            # engine.dispose()
            cc1 = pd.read_csv("/tmp/aa26.csv")
            print("cc1", cc1)
            print("cc1", cc1)
            ###for dynamic end date cal
            # end_date2 ='2021-03-30'
            print(end_date2)
            end_date4=end_date2[:-3]
            print(end_date4)
            last_date='26'
            final_date0=end_date4+last_date
            print('final_date0---',final_date0)
            final_date1=final_date0.replace("'", "")
            # print("cc1", cc1)
            data_data1 = cc1[cc1['work_date'] < final_date1]
            ###code commented
            # data_data1 = cc1[cc1['work_date'] < '2021-03-26']
            data_data1.to_csv('/tmp/z1999.csv', index=False)
            cc4455 = pd.read_csv("/tmp/z1999.csv")
            print("cc44", cc4455)
        else:
            ##create 5 empty csv file with headers
            df1.to_csv('/tmp/a32.csv', index=False)
            # df1.to_csv('a32.csv', index=False)
            dff = pd.read_csv("/tmp/a27.csv")
            dfff = dff.iloc[0:0]
            # dfff.to_csv('a300_test.csv', index=False)
            dfff.to_csv('/tmp/a300.csv', index=False)
            dfff.to_csv('/tmp/a310.csv', index=False)
            dfff.to_csv('/tmp/a311.csv', index=False)
            dfff.to_csv('/tmp/a312.csv', index=False)
            dfff.to_csv('/tmp/a313.csv', index=False)
            dfff.to_csv('/tmp/a314.csv', index=False)
            dfff.to_csv('/tmp/a320.csv', index=False)
            dfff.to_csv('/tmp/a321.csv', index=False)
            dfff.to_csv('/tmp/a322.csv', index=False)
            dfff.to_csv('/tmp/a323.csv', index=False)
            dfff.to_csv('/tmp/a324.csv', index=False)
            ##end create 5 empty csv file with headers
            print("data_random.empty====>", data_random.empty)
            if data_random.empty:
                pass
            else:
                a = 0
                for i in Week_Number1:
                    d = df1[['random_weekly_off', 'day_of_week', 'work_date', 'Week_Number', 'attandance',
                             'final_random_weekly_off']][df1['Week_Number'] == i]

                    d.to_csv('/tmp/a31' + str(a) + '.csv', index=False)
                    a = a + 1

            ##week one
            if os.stat('/tmp/a310.csv').st_size == 0:
                pass
            else:
                week1 = pd.read_csv("/tmp/a310.csv")
                data1 = week1[week1['attandance'] == 0]
                print(data1)
                if data1.empty:
                    print("true")
                    week1.to_csv('/tmp/a320.csv', index=False)
                else:
                    for ind in week1.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week1['attandance'][ind] == 0:
                            # print("if true")
                            week1['final_random_weekly_off'][ind] = 1
                            week1.to_csv('/tmp/a320.csv', index=False)
                            break

            ##week two
            if os.stat('/tmp/a311.csv').st_size == 0:
                pass
            else:
                week2 = pd.read_csv("/tmp/a311.csv")
                data2 = week2[week2['attandance'] == 0]
                print(data2)
                if data2.empty:
                    print("true")
                    week2.to_csv('/tmp/a321.csv', index=False)
                else:
                    for ind in week2.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week2['attandance'][ind] == 0:
                            # print("if true")
                            week2['final_random_weekly_off'][ind] = 1
                            week2.to_csv('/tmp/a321.csv', index=False)
                            break

            ##week three
            if os.stat('/tmp/a312.csv').st_size == 0:
                pass
            else:
                week3 = pd.read_csv("/tmp/a312.csv")
                data3 = week3[week3['attandance'] == 0]
                print(data3)
                if data3.empty:
                    print("true")
                    week3.to_csv('/tmp/a322.csv', index=False)
                else:
                    for ind in week3.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week3['attandance'][ind] == 0:
                            # print("if true")
                            week3['final_random_weekly_off'][ind] = 1
                            week3.to_csv('/tmp/a322.csv', index=False)
                            break

            ##week four
            if os.stat('/tmp/a313.csv').st_size == 0:
                pass
            else:
                week4 = pd.read_csv("/tmp/a313.csv")
                data4 = week4[week4['attandance'] == 0]
                print(data4)
                if data4.empty:
                    print("true")
                    week4.to_csv('/tmp/a323.csv', index=False)
                else:
                    for ind in week4.index:
                        # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                        if week4['attandance'][ind] == 0:
                            # print("if true")
                            week4['final_random_weekly_off'][ind] = 1
                            week4.to_csv('/tmp/a323.csv', index=False)
                            break

            ##week five
            if os.stat('/tmp/a314.csv').st_size == 0:
                pass
            else:
                week5 = pd.read_csv("/tmp/a314.csv")
                data5 = week5[week5['attandance'] == 0]
                print(data5)
                if data5.empty:
                    print("true")
                    week5.to_csv('/tmp/a324.csv', index=False)
                else:
                    data = (week5['attandance'] == 0).all()

                    print('data', data)
                    if data == False:
                        week5.to_csv('/tmp/a324.csv', index=False)
                    else:
                        week5 = pd.read_csv("/tmp/a314.csv")
                        print('week5------', week5)
                        for ind in week5.index:
                            # print(week1['attandance'][ind], week1['final_random_weekly_off'][ind])
                            if week5['attandance'][ind] == 0:
                                # print("if true")
                                week5['final_random_weekly_off'][ind] = 1
                                week5.to_csv('/tmp/a324.csv', index=False)
                                break

            ##appanding all split file in one file

            ddff = pd.read_csv("/tmp/a320.csv")
            ddff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddfff = pd.read_csv("/tmp/a321.csv")
            ddfff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddffff = pd.read_csv("/tmp/a322.csv")
            ddffff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddfffff = pd.read_csv("/tmp/a323.csv")
            ddfffff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ddffffff = pd.read_csv("/tmp/a324.csv")
            ddffffff.to_csv('/tmp/a300.csv', mode='a', index=False, header=False)

            ###final cal culation

            df555 = pd.read_csv('/tmp/a300.csv')
            ##for proper ordering

            df555.to_csv('/tmp/a301.csv', index=False)
            # df555.to_csv('a301.csv', index=False)

            ###updating random weeekly calculated data
            df115 = pd.read_csv("/tmp/a17.csv")
            df125 = pd.read_csv("/tmp/a301.csv")
            df225 = pd.merge(left=df115, right=df125, left_on='work_date', right_on='day_of_week')
            df225.to_csv('/tmp/aaa25.csv', index=False)

            df_final_cal = pd.read_csv("/tmp/aaa25.csv")

            df_final_cal.rename(columns={'final_random_weekly_off_y': 'final_random_weekly_off'}, inplace=True)

            df_final_cal.rename(columns={'final_random_weekly_off_y': 'final_random_weekly_off'}, inplace=True)
            ###test

            df_final_cal.to_csv('/tmp/aaa26.csv', index=False)
            # print("df_final_cal----",df_final_cal)
            df_final_cal.rename(columns={'attandance_x': 'attandance'}, inplace=True)
            df_final_cal.rename(columns={'work_date_x': 'work_date'}, inplace=True)
            df_final_cal.rename(columns={'day_of_week_x': 'day_of_week'}, inplace=True)
            ###weekly of field have to add
            df_final_cal['final_cal'] = df_final_cal['attandance'] + df_final_cal['final_saturday_sunday_off'] + \
                                        df_final_cal['final_sunday_off'] + df_final_cal['aproved_leave'] + df_final_cal[
                                            'final_random_weekly_off']
            df_final_cal['final'] = 0
            df_final_cal['final'] = np.where(df_final_cal['final_cal'] == 0, 0, 1)
            df_final_cal.to_csv('/tmp/a25.csv', index=False)

            ###final_csv file per employee per month
            emp_details = pd.read_csv('/tmp/a25.csv',
                                      usecols=['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs',
                                               'lop',
                                               'attandance', 'final_attandance', 'aproved_leave', 'ar_name'
                                          , 'final_random_weekly_off', 'final_sunday_off', 'final_saturday_sunday_off',
                                               'final'], low_memory=False)
            emp_details = emp_details[
                ['emp_id_id', 'attendance_id', 'name', 'work_date', 'day_of_week', 'work_hrs', 'lop', 'ar_name',
                 'attandance', 'final_attandance', 'aproved_leave'
                    , 'final_random_weekly_off', 'final_saturday_sunday_off', 'final_sunday_off', 'final']]

            emp_details.rename(columns={'final_attandance': 'attandance(attandance-lop)'}, inplace=True)
            emp_details.rename(columns={'final_random_weekly_off': 'random_weekly_off'}, inplace=True)
            emp_details.rename(columns={'final_saturday_sunday_off': 'saturday_sunday_off'}, inplace=True)
            emp_details.rename(columns={'final_sunday_off': 'sunday_off'}, inplace=True)
            emp_details.rename(columns={'final': 'resultant_data'}, inplace=True)
            emp_details.to_csv('/tmp/aab26.csv', index=False)
            data_final_final = pd.read_csv("/tmp/aab26.csv")
            data_final_final['resultant_data'] = data_final_final['resultant_data'] - data_final_final['lop']
            data_final_final.to_csv('/tmp/aa26.csv', index=False)
            # emp_details.to_csv('aa26.csv', index=False)
            # engine.dispose()
            cc1 = pd.read_csv("/tmp/aa26.csv")
            ###for dynamic end date cal
            # end_date2 ='2021-03-30'
            print(end_date2)
            end_date4=end_date2[:-3]
            print(end_date4)
            last_date='26'
            final_date0=end_date4+last_date
            print('final_date0---',final_date0)
            final_date1=final_date0.replace("'", "")
            # print("cc1", cc1)
            data_data1 = cc1[cc1['work_date'] < final_date1]
            ###code commented
            # data_data1 = cc1[cc1['work_date'] < '2021-03-26']
            data_data1.to_csv('/tmp/z1999.csv', index=False)
            cc4455 = pd.read_csv("/tmp/z1999.csv")
            print("cc44", cc4455)

            ###current month data
    ###now merging data
    cc4455xx.to_csv("/tmp/zzz11.csv",index=False)
    cc4455.to_csv("/tmp/zzz22.csv",index=False)
    ddffffff_final = pd.read_csv("/tmp/zzz22.csv")
    ddffffff_final.to_csv('/tmp/zzz11.csv', mode='a', index=False, header=False)
    cc4455_final = pd.read_csv("/tmp/zzz11.csv")
    cc4455_final.to_csv('/tmp/zzzz.csv',index=False)
    cc4455_final1 = pd.read_csv("/tmp/zzzz.csv")
    # print("cc4455_final", cc4455_final)

    engine.dispose()

    ##featch emp absent data only data
    ###commented  code------
    d_last = cc4455_final1[cc4455_final1['resultant_data'] < 1]

    # d_last = cc1[cc1['resultant_data'] < 1]
    d_last.to_csv('/tmp/z.csv')
    j = d_last.to_json(orient='records')
    data_data = j
    print("data_data==", data_data)
    # count no of absent days
    index = d_last.index
    number_of_rows = len(index)
    print("absent_days==>", number_of_rows)
    #####testing
    d_last = pd.read_csv('/tmp/z.csv')
    d_last['resultant_data'] = np.where(d_last['resultant_data'] == 0, '1', d_last['resultant_data'])
    d_last.to_csv('/tmp/z20.csv', index=False)
    d_last1 = pd.read_csv('/tmp/z20.csv')
    # j = d_last1.to_json(orient='records')
    # data_data = j
    print("data_data==", data_data)
    ##count no of absent days
    # index = d_last.index
    # number_of_rows = len(index)
    print("absent_days==>", number_of_rows)
    total1 = d_last1['resultant_data'].sum()
    print("total1=====>", total1)
    if total1 == 0:
        total2 = 0
    else:
        total2 = total1
    t = str(total2)

    engine.dispose()

    return {
        'status': True,
        'lop_count': t,
        'lop_days_record': json.loads(data_data)
    }

# print(lambda_handler())