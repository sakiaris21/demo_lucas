import sys
#from infrastructure.mysql import connection
import numpy as np
import logging
import pandas as pd
import mysql.connector
#from churn_functions.differences import apply_differences
#from churn_functions.finders import find_days_to_order
#from churn_functions.finders import find_churn_label

logger = logging.getLogger(__name__)


def car_new(username, groupby='customer_id', raw=False, account='all', local=False):
    '''
    '''

    sys.stdout.write("Calculating car...\r")
    dbase = "{}".format(username)
    try:
        con = mysql.connector.connect(
                                        host='localhost',
                                        user='root',
                                        password='sakis',
                                        database=dbase,
                                        charset='latin1',
                                        use_unicode=True,
                                        use_pure=True,
                                        port = '3307'
                                    )
        cur = con.cursor()

        query1 = f"\
            SELECT c1.customer_id, c1.name, IF(avgPriceYear IS NULL, 0, avgPriceYear) as avgPriceYTD, IF(avgPriceHalfYear IS NULL, 0, avgPriceHalfYear) as avgPriceHYTD, avgPrice2Year,\
            IF(avgPriceHalfYear=0, -1, IF(avgPriceHalfYear IS NULL, -1, IF(avgPriceYear=NULL, 1, ATAN2(avgPriceHalfYear-avgPriceYear, 185)/(PI()/2)))) as risk_avg\
            FROM\
            (SELECT customer_id, name FROM customers) c1\
            LEFT JOIN\
            (SELECT customer_id, AVG(sales.price) as avgPrice2Year FROM sales\
            WHERE DATE_SUB(CURDATE(),INTERVAL 2 YEAR) <= date\
            GROUP BY customer_id) a3\
            ON a3.customer_id=c1.customer_id\
            LEFT JOIN\
            (SELECT customer_id, AVG(sales.price) as avgPriceYear FROM sales\
            WHERE DATE_SUB(CURDATE(),INTERVAL 365 DAY) <= date\
            GROUP BY customer_id) a1\
            ON a1.customer_id=c1.customer_id\
            LEFT JOIN\
            (SELECT customer_id, AVG(sales.price) as avgPriceHalfYear FROM sales\
            WHERE DATE_SUB(CURDATE(),INTERVAL 180 DAY) <= date\
            GROUP BY customer_id) a2\
            ON a2.customer_id=c1.customer_id\
            \
            "

        query2 = f"""with df as (select *, DATE_SUB(CURDATE(),INTERVAL 120 DAY) date_120,
             DATE_SUB(CURDATE(),INTERVAL 240 DAY) as date_240,
              DATE_SUB(CURDATE(),INTERVAL 365 DAY) as date_360
             from sales) 
        select customers.customer_id, 
        IF(sumPrice8_4months IS NULL, 0, sumPrice8_4months) as sumPrice8_4months, 
        IF(sumPrice4months IS NULL, 0, sumPrice4months) as sumPrice4months, 
        sumPrice12_8months,
        IF(sumPrice4months=0, -1, IF(sumPrice4months IS NULL, -1, IF(sumPrice8_4months is NULL, 1,\
            cos(radians(90-(LEAST(IF(-90 < (100*((sumPrice4months-sumPrice8_4months)/sumPrice8_4months)),(100*((sumPrice4months-sumPrice8_4months)/sumPrice8_4months)), -90), 90)))))))  as risk_sum
        from customers
        left join (SELECT customer_id, SUM(df.price) as sumPrice4months FROM df
                    WHERE date >= date_120
                    GROUP BY customer_id) as a3
        on a3.customer_id = customers.customer_id
        left join (SELECT customer_id, SUM(df.price) as sumPrice8_4months FROM df
                    WHERE  date between date_240 and date_120
                    GROUP BY customer_id) as a1
        on a1.customer_id = customers.customer_id
        left join  (SELECT customer_id, SUM(df.price) as sumPrice12_8months
                    FROM df
                    WHERE date between date_360 and date_240
                    GROUP BY customer_id) as a2
        on a2.customer_id = customers.customer_id"""

        #

        query3 = f"""with df as (select *, DATE_SUB(CURDATE(),INTERVAL 120 DAY) date_120,
                    DATE_SUB(CURDATE(),INTERVAL 240 DAY) as date_240,
                    DATE_SUB(CURDATE(),INTERVAL 365 DAY) as date_360
                    from sales) 
        select customers.customer_id, 
        IF(sumQuantity8_4months IS NULL, 0, sumQuantity8_4months) as sumQuantity8_4months, 
        IF(sumQuantity4months IS NULL, 0, sumQuantity4months) as sumQuantity4months, 
        sumQuantity12_8months,
        IF(sumQuantity4months=0, -1, IF(sumQuantity4months IS NULL, -1, IF(sumQuantity8_4months is NULL, 1,\
            cos(radians(90-(LEAST(IF(-90 < (100*((sumQuantity4months-sumQuantity8_4months)/sumQuantity8_4months)),(100*((sumQuantity4months-sumQuantity8_4months)/sumQuantity8_4months)), -90), 90)))))))  as risk_quantity
        from customers
        left join (SELECT customer_id, SUM(df.quantity) as sumQuantity4months FROM df
                    WHERE date >= date_120
                    GROUP BY customer_id) as a3
        on a3.customer_id = customers.customer_id
        left join (SELECT customer_id, SUM(df.quantity) as sumQuantity8_4months FROM df
                    WHERE  date between date_240 and date_120
                    GROUP BY customer_id) as a1
        on a1.customer_id = customers.customer_id
        left join  (SELECT customer_id, SUM(df.quantity) as sumQuantity12_8months
                    FROM df
                    WHERE date between date_360 and date_240
                    GROUP BY customer_id) as a2
        on a2.customer_id = customers.customer_id"""

        
        query4 = f"""with df as (select *, DATE_SUB(CURDATE(),INTERVAL 120 DAY) date_120,
                    DATE_SUB(CURDATE(),INTERVAL 240 DAY) as date_240,
                    DATE_SUB(CURDATE(),INTERVAL 365 DAY) as date_360
                    from sales) 
        select customers.customer_id, 
        IF(counts8_4months IS NULL, 0, counts8_4months) as counts8_4months, 
        IF(counts4months IS NULL, 0, counts4months) as counts4months, 
        counts12_8months,
        IF(counts4months=0, -1, IF(counts4months IS NULL, -1, IF(counts8_4months is NULL, 1,\
            cos(radians(90-(LEAST(IF(-90 < (100*((counts4months-counts8_4months)/counts8_4months)),(100*((counts4months-counts8_4months)/counts8_4months)), -90), 90)))))))  as risk_total_transactions
        from customers
        left join (SELECT customer_id, count(df.quantity) as counts4months FROM df
                    WHERE date >= date_120
                    GROUP BY customer_id) as a3
        on a3.customer_id = customers.customer_id
        left join (SELECT customer_id, count(df.quantity) as counts8_4months FROM df
                    WHERE  date between date_240 and date_120
                    GROUP BY customer_id) as a1
        on a1.customer_id = customers.customer_id
        left join  (SELECT customer_id, COUNT(df.quantity) as counts12_8months
                    FROM df
                    WHERE date between date_360 and date_240
                    GROUP BY customer_id) as a2
        on a2.customer_id = customers.customer_id"""

        """load_data_query = f"select * from sales order by date"
        cur.execute(load_data_query)
        df = np.asarray(cur.fetchall())

        cols = [desc[0] for desc in cur.description]

        df = pd.DataFrame(df, columns=cols)#.drop(columns = 'index')
        df['quantity'] = df['quantity'].astype('int')
        df['cost'] = df['cost'].astype('float')
        df['date'] = pd.to_datetime(df['date'])
        df['price'] = df['price'].astype('float')
        df['margin'] = df['margin'].astype('float')
        df['kam_id'] = df['kam_id'].astype('int')
        df['unit_price'] = df['unit_price'].astype('float')
        #df2 = apply_differences(df = df, user_col = 'customer_id', date_col = 'date')
        #df3 = find_days_to_order(df2)
        #df_churn = find_churn_label(df3)[['customer_id', 'time_bounds', 'days_from_last_order', 'churn']]
        #df_churn_final =( df_churn[['customer_id', 'churn', 'time_bounds', 'days_from_last_order']]
         #           .groupby('customer_id')[[ 'churn', 'time_bounds', 'days_from_last_order']].last()
          #          .reset_index()"""
           #     )
        #query1
        sys.stdout.write("Done query1...\r")
        cur.execute(query1)
        df1 = np.asarray(cur.fetchall())
        cols = [desc[0] for desc in cur.description]
        df1 = pd.DataFrame(df1, columns=cols)
        df1['customer_id'] = df1['customer_id'].astype(str)
       # df = pd.merge(df1, df_churn_final, how='left', on = 'customer_id')
      #  df.loc[df.churn.isna(), 'churn'] = -1
        #query2
        cur.execute(query2)
        df2 = np.asarray(cur.fetchall())
        cols2 = [desc[0] for desc in cur.description]
        df2 = pd.DataFrame(df2, columns=cols2)
        df2['customer_id'] = df2['customer_id'].astype(str)
        # query3
        cur.execute(query3)
        df3 = np.asarray(cur.fetchall())
        cols3 = [desc[0] for desc in cur.description]
        df3 = pd.DataFrame(df3, columns=cols3)
        df3['customer_id'] = df3['customer_id'].astype(str)
        # query4
        cur.execute(query4)
        df4 = np.asarray(cur.fetchall())
        cols4 = [desc[0] for desc in cur.description]
        df4 = pd.DataFrame(df4, columns=cols4)

        sys.stdout.write("Query_4 Done...\r")
        df4['customer_id'] = df4['customer_id'].astype(str)
        df = (     df1
                    .merge(df2, how='inner', on = 'customer_id')
                    .merge(df3, how='inner', on = 'customer_id')
                    .merge(df4, how = 'inner', on = 'customer_id')
                   # .merge(df1, how = 'inner', on = 'customer_id')
                )
        df['risk'] = (df.apply(lambda x: (x['risk_avg']
                                                    #      + x['churn']
                                                          + x['risk_total_transactions']
                                                          + x['risk_sum']
                                                          + x['risk_quantity'])/5.0, axis = 1)
                                )
        
        sys.stdout.write("Calculating car...Done\n")
        return df#[['customer_id', 'avgPriceYTD', 'avgPriceHYTD', 'avgPrice2Year', 'risk']]
    except Exception as exception:
        logger.error(exception)
        raise








def risk_deploy(username, customer_id):
    '''
    '''

    #sys.stdout.write("Deploying risk...\r")
    dbase = "{}".format(username)
    try:
        con = mysql.connector.connect(
                                        host='localhost',
                                        user='root',
                                        password='sakis',
                                        database=dbase,
                                        charset='latin1',
                                        use_unicode=True,
                                        use_pure=True
                                    )
        cur = con.cursor()

        script_nop = f"""select risk from risk_table where customer_number = {customer_id}"""

        cur.execute(script_nop)
        data = np.asarray(cur.fetchall())
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=cols)
        #df['customer_number'] = df['customer_number'].astype(str)

        #logger.warning("Deploying risk...Done\n")
        return df.risk.values[0]
    except Exception as exception:
        logger.error(exception)
        raise




def churners_deploy(username, risk):
    '''
    '''

    #sys.stdout.write("Deploying risk...\r")
    dbase = "{}".format(username)
    try:
        con = mysql.connector.connect(
                                        host='localhost',
                                        user='root',
                                        password='sakis',
                                        database=dbase,
                                        charset='latin1',
                                        use_unicode=True,
                                        use_pure=True
                                    )
        cur = con.cursor()

        script_nop = f"""select customer_number from risk_table where risk < {risk}"""

        cur.execute(script_nop)
        data = np.asarray(cur.fetchall())
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=cols)
        #df['customer_number'] = df['customer_number'].astype(str)

        #logger.warning("Deploying risk...Done\n")
        return df.customer_number.values.tolist()
    except Exception as exception:
        logger.error(exception)
        raise
