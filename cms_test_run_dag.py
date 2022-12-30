# from itertools import groupby

# from numpy import kaiser
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from pathlib import Path

import sqlite3
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import psycopg2.extras as extras

import calendar
import uuid
import decimal
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import datetime as d
import os


# dag = DAG('commission', description='Calculate commission',
#           schedule_interval='0 13 * * *', start_date=datetime(2021, 1, 14), catchup=False)

# define enum from main-container
payout_frequency_weekly = 'weekly'
payout_frequency_bi_monthly = 'bi_monthly'
payout_frequency_monthly = 'monthly'

weeklydb_path = './data/temp_commission_weekly.db'
bimonthlydb_path = './data/temp_commission_bimonthly.db'
monthlydb_path = './data/temp_commission_monthly.db'

commission_fee_type_dp = 1
commission_fee_type_wd = 2
commission_fee_type_adj = 3

deposit_status_cuccessful = 3
withdrawal_status_cuccessful = 6
adjustment_status_cuccessful = 2
adjustment_type_commission = 1


adjustment_transaction_type_credit = 3
adjustment_transaction_type_debit = 4


member_status_active = 1

commission_status_processing = 1
commission_status_threshold = 2
commission_status_rejected = 3
commission_status_approve = 4
commission_status_release = 5

commission_payment_status_processing = 1
commission_payment_status_threshold = 2
commission_payment_status_success = 3

commission_fee_table = 'cm_commission_fee'
member_wager_product_table = 'cm_member_wager_product'
commission_summary_table = 'cm_commission_summary'
commission_table = 'cm_commission'
affiliate_account_table = 'affiliate_account'
commission_settlement_log_table = 'commission_settlement_log'

default_commission_tier1 = 28
default_commission_tier2 = 38
default_commission_tier3 = 48
default_payout_frequency = 'monthly'
default_min_active_player = 5
default_platform_fee = 2
default_commission_min_net = 100
default_commission_min_net_tier1 = 0
default_commission_max_net_tier1 = 10000
default_commission_min_net_tier2 = 10001
default_commission_max_net_tier2 = 100000
default_commission_min_net_tier3 = 100001

default_commission_usd_usd_rate = 1
default_commission_usd_vnd_rate = 23
default_commission_usd_rmb_rate = 6.5
default_commission_usd_thb_rate = 30

USD = 'USD'
VND = 'VND'
RMB = 'RMB'
THB = 'THB'

#
# conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
# conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
# conn_identity_pg_hook = PostgresHook(postgres_conn_id='identity_conn_id')
# conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
# engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

chunksize = 10000

# from main-container
AllBet = "allbet"
AG = "asiagaming"
AGSlot = "agslot"
AGYoplay = "agyoplay"
Sagaming = "sagaming"
SPSlot = "simpleplay"
SPFish = "simpleplayfisher"
SabaCV = "sabacv"
PGsoft = "pgsoft"
EBetgaming = "ebetgaming"
Betradar = "betradar"
Btisports = "btisports"
TFgaming = "tfgaming"
Evolution = "evolution"
Genesis = "genesis"
Saba = "saba"
SabaNumberGame = "sabanumbergames"
SabaVirtual = "sabavirtual"
Digitain = "digitain"
#

EuroValue = 1.65
HongkongValue = 0.65
MalayValue = 0.65
IndoValue = -1.54
UsValue = -154
#


param_dic_iden = {
    'host': 'localhost',
    'database': 'identityDB',
    'user': 'postgres',
    'password': 'secret'
}

param_dic_aff = {
    'host': 'localhost',
    'database': 'affiliateDB',
    'user': 'postgres',
    'password': 'secret'
}

param_dic_payment = {
    'host': 'localhost',
    'database': 'paymentDB',
    'user': 'postgres',
    'password': 'secret'
}

param_dic_wag = {
    'host': 'localhost',
    'database': 'collectorDB',
    'user': 'postgres',
    'password': 'secret'
}


def connectToDB(param_dic):
    conn = None
    try:
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn


def getData(conn, select_query, data, column_names):
    cursor = conn.cursor()
    try:
        cursor.execute(select_query, data)
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        cursor.close()
        return 1

    tupples = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(tupples, columns=column_names)
    return df


def deleteData(conn, delete_query, data):
    cursor = conn.cursor()
    try:
        cursor.execute(delete_query, data)
        conn.commit()
        count = cursor.rowcount
        # print(count, 'Record deleted successfully ')
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error in Delete operation', error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            # print('PostgreSQL connection is closed')


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = 'INSERT INTO %s(%s) VALUES %%s RETURNING id' % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        res = cursor.fetchall()
        # last_inserted_id = res[0]

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        conn.rollback()
        cursor.close()

        return 1
    return res


def updateTable(conn, sql_update_query, data):
    cursor = conn.cursor()
    # print("Record successfully")
    try:
        cursor.execute(sql_update_query, data)
        conn.commit()
        count = cursor.rowcount
        # print(count, "Record Updated successfully ")

    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)
        cursor.close()
        return 1


def extractAffiliate(payout_frequency):
    connAffiliate = connectToDB(param_dic_aff)
    column_names_affiliate = ['affiliate_id', 'affiliate_name',
                              'commission_tier1', 'commission_tier2', 'commission_tier3',
                              'payout_frequency', 'min_active_player']
    rawsql = \
        """
          SELECT
                affiliate_id,
                login_name as affiliate_name,
                commission_tier1,
                commission_tier2,
                commission_tier3,
                payout_frequency,
                min_active_player
          FROM affiliate_account
          WHERE payout_frequency = '{0}'
        """.format(payout_frequency)
    dfAff = getData(connAffiliate, rawsql, None, column_names_affiliate)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # dfAff = conn_affiliate_pg_hook.get_pandas_df(rawsql)
    dfAff['commission_tier1'].fillna(default_commission_tier1, inplace=True)
    dfAff['commission_tier2'].fillna(default_commission_tier2, inplace=True)
    dfAff['commission_tier3'].fillna(default_commission_tier3, inplace=True)
    dfAff['payout_frequency'].fillna(default_payout_frequency, inplace=True)
    dfAff['min_active_player'].fillna(default_min_active_player, inplace=True)
    return dfAff


def extractIdentity():
    connIdentiy = connectToDB(param_dic_iden)
    column_names_identity = ['member_id',
                             'member_name', 'affiliate_id', 'm_currency']
    rawsql = \
        """
        SELECT
            id as member_id,
            login_name as member_name,
            affiliate_id,
            currency as m_currency
        FROM member
        WHERE
            affiliate_id != 0
            AND status = {0}
        """.format(member_status_active)
    dfIden = getData(connIdentiy, rawsql, None, column_names_identity)
    # conn_identity_pg_hook = PostgresHook(postgres_conn_id='identity_conn_id')
    # dfIden = conn_identity_pg_hook.get_pandas_df(rawsql)
    return dfIden


def extractCollector(datefrom, dateto, dfIden, uuids, frequency):
    rawsql =\
        """ 
                SELECT 
                        login_name as member_name,
                        product,
                        sum(stake) as total_stake,
                        sum(count) as total_count,
                        sum(win_loss) as total_win_loss
                FROM wager_summary
                GROUP BY
                        product,login_name 
            """

    if frequency == payout_frequency_monthly:
        if os.path.exists(monthlydb_path):
            sqlite_conn_temp_commission_monthly = sqlite3.connect(
                monthlydb_path)
            dfWager = pd.read_sql_query(
                rawsql, sqlite_conn_temp_commission_monthly)
            df = dfWager.merge(dfIden, how='inner', on='member_name')
            # => column_names_collector + member_id, affiliate_id
            saveMemberWagerProduct(df, datefrom, dateto, uuids)
            return df

    elif frequency == payout_frequency_bi_monthly:
        if os.path.exists(bimonthlydb_path):
            sqlite_conn_temp_commission_bimonthly = sqlite3.connect(
                bimonthlydb_path)
            dfWager = pd.read_sql_query(
                rawsql, sqlite_conn_temp_commission_bimonthly)
            df = dfWager.merge(dfIden, how='inner', on='member_name')
            # => column_names_collector + member_id, affiliate_id
            saveMemberWagerProduct(df, datefrom, dateto, uuids)
            return df

    else:
        dfWager = pd.DataFrame(None, columns=[
                               'member_name', 'product', 'total_stake', 'total_count', 'total_win_loss'])
        df = dfWager.merge(dfIden, how='inner', on='member_name')
        return df


def clean_sqlite_temp_commission(frequency):
    if frequency == payout_frequency_monthly:
        clean_sqlite_temp_commission_monthly()
    if frequency == payout_frequency_bi_monthly:
        clean_sqlite_temp_bi_commission_monthly()


def clean_sqlite_temp_commission_monthly():
    if os.path.exists(monthlydb_path):
        # cur = sqlite_conn.cursor()
        # cur.execute("drop table if exists wager_summary")
        # sqlite_conn.commit()
        os.remove(monthlydb_path)


def clean_sqlite_temp_bi_commission_monthly():
    if os.path.exists(bimonthlydb_path):
        # cur = sqlite_conn.cursor()
        # cur.execute("drop table if exists wager_summary")
        # sqlite_conn.commit()
        os.remove(bimonthlydb_path)


def extractDeposit(datefrom, dateto, uuids, dfAI):
    connPayment = connectToDB(param_dic_payment)

    column_name_deposit = ['member_name', 'member_id', 'transaction_id',
                           'amount', 'm_currency', 'transaction_date',
                           'payment_type_rate', 'payment_type_code',
                           'affiliate_id', 'type']

    rawsql = \
        """
        SELECT
            d.login_name as member_name,
            ma.member_id,
            d.transaction_id,
            d.net_amount as amount,
            d.currency as m_currency,
            d.create_at as transaction_date,
            pt.affiliate_fee as payment_type_rate,
            d.payment_type_code,
            ma.affiliate_id,
            {0} as type
        FROM deposit as d
        LEFT JOIN payment_type as pt on d.payment_type_code = pt.code
        LEFT JOIN member_account as ma on d.login_name = ma.login_name
        WHERE
            ma.affiliate_id != 0
            AND d.status = {1}
            AND d.create_at BETWEEN '{2}' AND '{3}'
            AND pt.affiliate_fee > 0
        """.format(commission_fee_type_dp, deposit_status_cuccessful, datefrom, dateto)

    dfDeposit = getData(connPayment, rawsql, None, column_name_deposit)
    # conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    # dfDeposit = conn_payment_pg_hook.get_pandas_df(rawsql)

    dfDeposit = dfDeposit.merge(dfAI, how='inner', on='affiliate_id')
    dfDeposit = dfDeposit.rename(columns={'m_currency': 'currency'})
    saveCommissionFeeTable(
        dfDeposit, commission_fee_type_dp, datefrom, dateto, uuids)
    return dfDeposit


def extractWithdrawal(datefrom, dateto, uuids, dfAI):
    connPayment = connectToDB(param_dic_payment)

    column_name_withdrawal = ['member_name', 'member_id', 'transaction_id',
                              'amount', 'm_currency', 'transaction_date',
                              'payment_type_rate', 'payment_type_code',
                              'affiliate_id', 'type']

    rawsql =\
        """
        SELECT
            d.login_name as member_name,
            ma.member_id,
            d.transaction_id,
            d.withdrawal_amount as amount,
            d.currency as m_currency,
            d.create_at as transaction_date,
            pt.affiliate_fee as payment_type_rate,
            d.payment_type_code,
            ma.affiliate_id,
            {0} as type
        FROM withdrawal as d
        LEFT JOIN payment_type as pt on d.payment_type_code = pt.code
        LEFT JOIN member_account as ma on d.login_name =ma.login_name
        WHERE
            ma.affiliate_id !=0
            AND d.status = {1}
            AND d.create_at BETWEEN '{2}' AND '{3}'
            AND pt.affiliate_fee > 0
        """.format(commission_fee_type_wd, withdrawal_status_cuccessful, datefrom, dateto)

    dfWithdrawal = getData(connPayment, rawsql, None, column_name_withdrawal)
    # conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    # dfWithdrawal = conn_payment_pg_hook.get_pandas_df(rawsql)
    dfWithdrawal = dfWithdrawal.merge(dfAI, how='inner', on='affiliate_id')
    dfWithdrawal = dfWithdrawal.rename(columns={'m_currency': 'currency'})
    saveCommissionFeeTable(
        dfWithdrawal, commission_fee_type_wd, datefrom, dateto, uuids)
    return dfWithdrawal


def extractAdjustment(datefrom, dateto, uuids, dfAI):
    connPayment = connectToDB(param_dic_payment)

    column_name_adjustment = ['member_name', 'member_id', 'transaction_id',
                              'amount', 'm_currency', 'transaction_date',
                              'affiliate_id', 'type', 'payment_type_rate',
                              'adjustment_reason_name']

    rawsql =\
        """
        SELECT
            a.login_name as member_name,
            ma.member_id,
            a.transaction_id,
            (CASE WHEN a.transaction_type = {0} THEN a.amount ELSE -a.amount END) as amount,
            a.currency as m_currency,
            a.create_at as transaction_date,
            ma.affiliate_id,
            {1} as type,
            100 as payment_type_rate,
            ar.name as adjustment_reason_name
        FROM adjustment as a
        LEFT JOIN adjustment_reason as ar on a.reason_id = ar.id
        LEFT JOIN member_account as ma on a.login_name = ma.login_name
        WHERE
            a.status = {2}
            AND ar.is_charge_to_affiliate = true
            AND a.create_at BETWEEN '{3}' AND '{4}'
            AND ma.affiliate_id != 0
        """.format(adjustment_transaction_type_credit, commission_fee_type_adj, adjustment_status_cuccessful, datefrom, dateto)

    dfAdjustment = getData(connPayment, rawsql, None, column_name_adjustment)
    # conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    # dfAdjustment = conn_payment_pg_hook.get_pandas_df(rawsql)
    dfAdjustment['remark'] = dfAdjustment['adjustment_reason_name'] + \
        ': ' + dfAdjustment['amount'].astype(str)
    dfAdjustment = dfAdjustment.merge(dfAI, how='inner', on='affiliate_id')
    dfAdjustment = dfAdjustment.rename(columns={'m_currency': 'currency'})
    saveCommissionFeeTable(
        dfAdjustment, commission_fee_type_adj, datefrom, dateto, uuids)
    return dfAdjustment


def get_adj_amount(row):
    amount = decimal.Decimal(row.amount) / row.usd_rate
    return amount


def extractAffiliateAdjustment(datefrom, dateto):
    connAffiliate = connectToDB(param_dic_aff)
    column_name_adjustment = ['amount', 'affiliate_id', 'currency']
    rawsql =\
        """
        SELECT
            (CASE WHEN a.transaction_type = {0} THEN a.amount ELSE -a.amount END) as amount,
            a.affiliate_id,
            a.currency
        FROM adjustment as a
        WHERE
            a.status = {1}
            AND a.create_at BETWEEN '{2}' AND '{3}'
            AND a.type = {4}
        """.format(adjustment_transaction_type_credit, adjustment_status_cuccessful, datefrom, dateto, adjustment_type_commission)

    dfAdjustment = getData(connAffiliate, rawsql, None, column_name_adjustment)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # dfAdjustment = conn_affiliate_pg_hook.get_pandas_df(rawsql)
    dfAdjustment = dfAdjustment.groupby(
        ['affiliate_id', 'currency'], as_index=False).agg({'amount': 'sum'})

    if len(dfAdjustment) > 0:
        dfAdjustment['usd_rate'] = dfAdjustment.apply(get_usd_rate,  axis=1)
    else:
        dfAdjustment['usd_rate'] = decimal.Decimal(
            default_commission_usd_usd_rate)

    if len(dfAdjustment) > 0:
        dfAdjustment['amount'] = dfAdjustment.apply(get_adj_amount,  axis=1)

    dfAdjustment = dfAdjustment.rename(columns={'amount': 'total_adjustment'})
    dfAdjustment = dfAdjustment.loc[:, ['affiliate_id', 'total_adjustment']]
    return dfAdjustment


def extractAffiliateSetting():
    connAffiliate = connectToDB(param_dic_aff)
    column_name_setting = ['id', 'key', 'name',
                           'value', 'type', 'create_at', 'create_by']
    rawsql = \
        """
        SELECT
            id,
            key,
            name,
            value,
            type,
            create_at,
            create_by
        FROM setting
        """
    dfSetting = getData(connAffiliate, rawsql, None, column_name_setting)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # dfSetting = conn_affiliate_pg_hook.get_pandas_df(rawsql)
    commission_tier1 = dfSetting.loc[dfSetting['key'] == 'COMMISSION_TIER1']
    global default_commission_tier1
    default_commission_tier1 = commission_tier1['value'].values[0]

    global default_commission_tier2
    comission_tier2 = dfSetting.loc[dfSetting['key'] == 'COMMISSION_TIER2']
    default_commission_tier2 = comission_tier2['value'].values[0]

    global default_commission_tier3
    comission_tier3 = dfSetting.loc[dfSetting['key'] == 'COMMISSION_TIER3']
    default_commission_tier3 = comission_tier3['value'].values[0]

    global default_payout_frequency
    payout_frequency = dfSetting.loc[dfSetting['key']
                                     == 'DEFAULT_PAYOUT_FREQUENCY']
    default_payout_frequency = payout_frequency['value'].values[0]

    global default_min_active_player
    min_active_player = dfSetting.loc[dfSetting['key']
                                      == 'COMMISSION_MIN_ACTIVE_PLAYER']
    default_min_active_player = min_active_player['value'].values[0]

    global default_platform_fee
    platform_fee = dfSetting.loc[dfSetting['key'] == 'COMMISSION_PLATFORM_FEE']
    default_platform_fee = platform_fee['value'].values[0]

    global default_commission_min_net_tier1
    min_net_tier1 = dfSetting.loc[dfSetting['key']
                                  == 'COMMISSION_MIN_NET_TIER1']
    default_commission_min_net_tier1 = min_net_tier1['value'].values[0]

    global default_commission_max_net_tier1
    max_net_tier1 = dfSetting.loc[dfSetting['key']
                                  == 'COMMISSION_MAX_NET_TIER1']
    default_commission_max_net_tier1 = max_net_tier1['value'].values[0]

    global default_commission_min_net_tier2
    min_net_tier2 = dfSetting.loc[dfSetting['key']
                                  == 'COMMISSION_MIN_NET_TIER2']
    default_commission_min_net_tier2 = min_net_tier2['value'].values[0]

    global default_commission_max_net_tier2
    max_net_tier2 = dfSetting.loc[dfSetting['key']
                                  == 'COMMISSION_MAX_NET_TIER2']
    default_commission_max_net_tier2 = max_net_tier2['value'].values[0]

    global default_commission_min_net_tier3
    min_net_tier3 = dfSetting.loc[dfSetting['key']
                                  == 'COMMISSION_MIN_NET_TIER3']
    default_commission_min_net_tier3 = min_net_tier3['value'].values[0]

    global default_commission_usd_usd_rate
    usd = dfSetting.loc[dfSetting['key'] == 'USD_USD_RATE']
    default_commission_usd_usd_rate = usd['value'].values[0]

    global default_commission_usd_vnd_rate
    vnd = dfSetting.loc[dfSetting['key'] == 'USD_VND_RATE']
    default_commission_usd_vnd_rate = vnd['value'].values[0]

    global default_commission_usd_rmb_rate
    rmb = dfSetting.loc[dfSetting['key'] == 'USD_RMB_RATE']
    default_commission_usd_rmb_rate = rmb['value'].values[0]

    global default_commission_usd_thb_rate
    thb = dfSetting.loc[dfSetting['key'] == 'USD_THB_RATE']
    default_commission_usd_thb_rate = thb['value'].values[0]

    return dfSetting


def extractAffiliateCommission(datefrom, dateto, frequency):
    connAffiliate = connectToDB(param_dic_aff)
    column_name = ['affiliate_id', 'previous_settlement']

    rawsql =\
        """
        SELECT
            DISTINCT ON(cm.affiliate_id) cm.affiliate_id,
            cm.rollover_next_month as previous_settlement
        FROM {0} as cm
		LEFT JOIN affiliate_account as aa ON aa.affiliate_id = cm.affiliate_id
        WHERE
            commission_status = {1}
            AND (from_transaction_date != '{2}' AND to_transaction_date != '{3}')
            AND to_transaction_date <= '{4}'
			AND (cm.total_members_stake >= aa.min_active_player or (aa.min_active_player IS NULL AND cm.total_members_stake >= {5}))
  	    ORDER BY cm.affiliate_id,
            to_transaction_date desc
        """.format(commission_table, commission_status_threshold, datefrom, dateto, dateto, default_min_active_player)

    df = getData(connAffiliate, rawsql, None, column_name)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # df = conn_affiliate_pg_hook.get_pandas_df(rawsql)
    df = df.drop_duplicates(subset='affiliate_id', keep="first")
    return df


def calculate_commission_fee(row):
    deposit = 0
    withdraw = 0
    expenses = 0
    otherFee = 0
    if row['type'] == commission_fee_type_wd:
        withdraw = row['amount']
        expenses = row['fee_amount']
    if row['type'] == commission_fee_type_dp:
        deposit = row['amount']
        expenses = row['fee_amount']
    if row['type'] == commission_fee_type_adj:
        otherFee = row['fee_amount']

    return pd.Series((deposit, withdraw, expenses, otherFee))


def calculate_member_aff_fee(dataDeposit, dataWithdraw, dataAdjustment):
    col = ['member_name', 'transaction_date', 'transaction_id', 'amount',
           'payment_type_rate', 'type', 'currency', 'affiliate_id', 'usd_rate']
    dataDeposit = dataDeposit.loc[:, col]
    dataWithdraw = dataWithdraw.loc[:, col]
    dataAdjustment = dataAdjustment.loc[:, col]
    feeData = pd.concat(
        [dataDeposit, dataWithdraw, dataAdjustment]).reset_index(drop=True)
    feeData['fee_amount'] = feeData['amount'] * \
        feeData['payment_type_rate']/100
    feeData = feeData.groupby(['member_name', 'type', 'currency', 'affiliate_id', 'usd_rate'], as_index=False).agg(
        {'fee_amount': 'sum', 'amount': 'sum'})

    if len(feeData) > 0:
        feeData[['deposit', 'withdraw', 'expenses', 'other_fee']] = feeData.apply(
            calculate_commission_fee,  axis=1)
    else:
        feeData = feeData.assign(
            deposit=0, withdraw=0, expenses=0, other_fee=0)

    feeData = feeData.loc[:, ['member_name', 'deposit',
                              'withdraw', 'expenses', 'other_fee', 'affiliate_id']]
    feeData = feeData.groupby(['member_name', 'affiliate_id'], as_index=False).agg(
        {'deposit': 'sum', 'withdraw': 'sum', 'expenses': 'sum', 'other_fee': 'sum'})
    return feeData


def calculate_member_aff_wager_product(dfProduct):
    dfProduct = dfProduct.groupby(['member_name', 'affiliate_id'], as_index=False).agg(
        {'total_win_loss': 'sum', 'total_stake': 'sum'})
    return dfProduct


def sum_m_amount(row):
    deposit_amount = decimal.Decimal(row['deposit_amount'])/row['usd_rate']
    withdrawal_amount = decimal.Decimal(
        row['withdrawal_amount'])/row['usd_rate']
    expenses = decimal.Decimal(row['expenses'])/row['usd_rate']
    other_fee = decimal.Decimal(row['other_fee'])/row['usd_rate']
    company_win_loss = decimal.Decimal(row['company_win_loss'])/row['usd_rate']
    total_stake = decimal.Decimal(row['total_stake'])/row['usd_rate']
    return pd.Series((deposit_amount, withdrawal_amount, expenses, other_fee, company_win_loss, total_stake))


def calculate_member_aff_commission_summary(memberFee, membeWager, dfIden, uuids, datefrom, dateto):
    df = memberFee.merge(membeWager, how='outer', on=[
                         'member_name', 'affiliate_id'])
    df = df.merge(dfIden, how='inner', on=['member_name'])
    df['deposit'].fillna(0, inplace=True)
    df['withdraw'].fillna(0, inplace=True)
    df['expenses'].fillna(0, inplace=True)
    df['other_fee'].fillna(0, inplace=True)
    df['total_win_loss'].fillna(0, inplace=True)
    df['total_stake'].fillna(0, inplace=True)
    df['total_win_loss'] = -df['total_win_loss']

    if len(df) > 0:
        df['m_usd_rate'] = df.apply(get_m_usd_rate,  axis=1)
    else:
        df['m_usd_rate'] = decimal.Decimal(default_commission_usd_usd_rate)

    df = df.rename(columns={'deposit': 'deposit_amount', 'withdraw': 'withdrawal_amount',
                   'total_win_loss': 'company_win_loss', 'm_currency': 'currency', 'm_usd_rate': 'usd_rate'})
    saveCommissionSummary(df, datefrom, dateto, uuids)

    if len(df) > 0:
        df[['deposit_amount', 'withdrawal_amount', 'expenses', 'other_fee', 'company_win_loss', 'total_stake']] = df.apply(
            sum_m_amount,  axis=1)
    else:
        df = df.assign(deposit_amount=0, withdrawal_amount=0,
                       other_fee=0, company_win_loss=0, total_stake=0)

    df2 = df.loc[:, ['affiliate_id', 'member_id', 'member_name', 'deposit_amount',
                     'withdrawal_amount', 'expenses', 'other_fee', 'company_win_loss', 'total_stake']]
    return df2


def calculate_total(row):
    platform_fee = decimal.Decimal(
        row.company_win_loss) * decimal.Decimal(default_platform_fee/100)

    temp_platform_fee = 0
    if platform_fee > 0:
        temp_platform_fee = platform_fee

    fee_total = decimal.Decimal(
        row.expenses) + decimal.Decimal(row.other_fee) + temp_platform_fee

    net_company_win_loss = decimal.Decimal(
        row.company_win_loss) - decimal.Decimal(fee_total)
    tier = 0
    total_amount = 0  # Total Affiliate (USD) Commission
    grand_total = 0
    rollover_next_month = 0
    commission_status = commission_status_threshold
    payment_status = commission_payment_status_threshold

    # cal
    if net_company_win_loss <= default_commission_max_net_tier1:
        tier = row['commission_tier1']
        total_amount = net_company_win_loss * decimal.Decimal(tier/100)
    else:
        if net_company_win_loss - default_commission_max_net_tier1 > default_commission_min_net_tier3:
            tier = row['commission_tier3']
            total_amount = default_commission_max_net_tier1 * decimal.Decimal(row['commission_tier1']/100) + default_commission_max_net_tier2 * decimal.Decimal(
                row['commission_tier2']/100) + (net_company_win_loss-default_commission_max_net_tier1-default_commission_max_net_tier2) * decimal.Decimal(tier/100)
        else:
            tier = row['commission_tier2']
            total_amount = default_commission_max_net_tier1 * decimal.Decimal(row['commission_tier1']/100) + (
                net_company_win_loss-default_commission_max_net_tier1) * decimal.Decimal(tier/100)

    grand_total = total_amount + \
        decimal.Decimal(row.total_adjustment) + \
        decimal.Decimal(row.previous_settlement)
    if row['total_members_stake'] < row['min_active_player']:
        commission_status = commission_status_threshold
        payment_status = commission_payment_status_threshold
        rollover_next_month = 0
    else:
        if grand_total < default_commission_min_net:
            commission_status = commission_status_threshold
            payment_status = commission_payment_status_threshold
            rollover_next_month = grand_total
        else:
            commission_status = commission_status_processing
            payment_status = commission_payment_status_processing
            rollover_next_month = 0

    return pd.Series((tier, total_amount, grand_total, fee_total, net_company_win_loss, rollover_next_month, commission_status, payment_status, platform_fee))


def calculate_commission(dfIdentity, dfAffiliate, memberSummaryDetail, dfAffAdj, datefrom, dateto, uuids, preAffCommission):
    # st1
    # tota member with commission rule
    ttmb = dfIdentity.merge(dfAffiliate, how='inner', on='affiliate_id')
    ttmb = ttmb.groupby(['affiliate_id', 'affiliate_name', 'commission_tier1',
                         'commission_tier2', 'commission_tier3', 'payout_frequency', 'min_active_player']).size().reset_index(name='total_members')

    # st2
    # tota member stake
    if len(memberSummaryDetail) > 0:
        memberSummaryDetail['total_members_stake'] = memberSummaryDetail.apply(
            lambda x: 1 if x['total_stake'] > 0 else 0, axis=1)
    else:
        memberSummaryDetail['total_members_stake'] = 0

    ttms = memberSummaryDetail.groupby(['affiliate_id']).agg({'deposit_amount': 'sum', 'withdrawal_amount': 'sum',
                                                              'expenses': 'sum', 'other_fee': 'sum', 'company_win_loss': 'sum', 'total_stake': 'sum', 'total_members_stake': 'sum'}).reset_index(drop=False)

    # st3
    # merge member, aff with affiliate_adj
    ttmb = ttmb.merge(dfAffAdj, how='left', on=['affiliate_id'])
    ttmb['total_adjustment'].fillna(0, inplace=True)

    # st4
    # summary all
    df = ttmb.merge(ttms, how='inner', on=['affiliate_id'])

    # st5
    # merge pre aff commission
    df = df.merge(preAffCommission, how='left', on='affiliate_id')
    df['previous_settlement'].fillna(0, inplace=True)

    if len(df) > 0:
        df[['tier', 'total_amount', 'grand_total', 'fee_total', 'net_company_win_loss',
            'rollover_next_month', 'commission_status', 'payment_status', 'platform_fee']] = df.apply(calculate_total, axis=1)
    else:
        df = df.assign(tier=0, total_amount=0, grand_total=0, fee_total=0, net_company_win_loss=0,
                       rollover_next_month=0, commission_status=commission_status_threshold, payment_status=commission_payment_status_threshold, platform_fee=0)

    df['currency'] = USD
    df['usd_rate'] = default_commission_usd_usd_rate

    df = df.loc[:, ['affiliate_id', 'affiliate_name', 'currency', 'usd_rate', 'total_members', 'total_members_stake', 'total_stake',
                    'company_win_loss', 'total_adjustment', 'expenses', 'net_company_win_loss', 'previous_settlement', 'tier',
                    'total_amount', 'grand_total', 'rollover_next_month', 'commission_status', 'payment_status', 'payout_frequency', 'other_fee', 'platform_fee']]
    saveCommissionTable(df, datefrom, dateto, uuids)
    return df


def get_usd_rate(row):
    usd_rate = decimal.Decimal(default_commission_usd_usd_rate)
    if row['currency'] == VND:
        usd_rate = decimal.Decimal(default_commission_usd_vnd_rate)
    if row['currency'] == THB:
        usd_rate = decimal.Decimal(default_commission_usd_thb_rate)
    if row['currency'] == RMB:
        usd_rate = decimal.Decimal(default_commission_usd_rmb_rate)
    return usd_rate


def get_m_usd_rate(row):
    usd_rate = decimal.Decimal(default_commission_usd_usd_rate)
    if row['m_currency'] == VND:
        usd_rate = decimal.Decimal(default_commission_usd_vnd_rate)
    if row['m_currency'] == THB:
        usd_rate = decimal.Decimal(default_commission_usd_thb_rate)
    if row['m_currency'] == RMB:
        usd_rate = decimal.Decimal(default_commission_usd_rmb_rate)
    return usd_rate


def saveCommissionFeeTable(df, type_cf, datefrom, dateto, uuids):
    if len(df) > 0:
        df['usd_rate'] = df.apply(get_usd_rate,  axis=1)
    else:
        df['usd_rate'] = decimal.Decimal(default_commission_usd_usd_rate)

    df['commission_status'] = commission_status_processing
    df['uuid'] = uuids

    # delete data
    rawsql =\
        """
        DELETE from {0}
        WHERE
            transaction_date BETWEEN '{1}' AND '{2}'
            AND type = {3}
            AND commission_status < {4}
        """.format(commission_fee_table, datefrom, dateto, type_cf, commission_status_approve)

    connAff = connectToDB(param_dic_aff)
    deleteData(connAff, rawsql, None)
    connAff = connectToDB(param_dic_aff)
    execute_values(connAff, df, commission_fee_table)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

    # conn_affiliate_pg_hook.run(rawsql)
    # df.to_sql(commission_fee_table, con=engine_affiliate, if_exists='append', index=False)


def saveMemberWagerProduct(df, datefrom, dateto, uuids):
    df = df.rename(columns={'m_currency': 'currency'})
    if len(df) > 0:
        df['usd_rate'] = df.apply(get_usd_rate,  axis=1)
    else:
        df['usd_rate'] = decimal.Decimal(default_commission_usd_usd_rate)

    df['commission_status'] = commission_status_processing
    df['uuid'] = uuids
    df['from_transaction_date'] = datefrom
    df['to_transaction_date'] = dateto

    # delete data
    rawsql =\
        """
        DELETE from {0}
        WHERE
            from_transaction_date = '{1}'
            AND to_transaction_date = '{2}'
            AND commission_status < {3}
        """.format(member_wager_product_table, datefrom, dateto, commission_status_approve)

    connAff = connectToDB(param_dic_aff)
    deleteData(connAff, rawsql, None)
    connAff = connectToDB(param_dic_aff)
    execute_values(connAff, df, member_wager_product_table)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

    # conn_affiliate_pg_hook.run(rawsql)
    # df.to_sql(member_wager_product_table, con=engine_affiliate, if_exists='append', index=False)


def saveCommissionSummary(df, datefrom, dateto, uuids):
    df['commission_status'] = commission_status_processing
    df['uuid'] = uuids
    df['from_transaction_date'] = datefrom
    df['to_transaction_date'] = dateto

    # delete data
    rawsql =\
        """
        DELETE from {0}
        WHERE
            from_transaction_date  = '{1}'
            AND to_transaction_date = '{2}'
            AND commission_status < {3}
        """.format(commission_summary_table, datefrom, dateto, commission_status_approve)

    connAff = connectToDB(param_dic_aff)
    deleteData(connAff, rawsql, None)
    connAff = connectToDB(param_dic_aff)
    execute_values(connAff, df, commission_summary_table)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

    # conn_affiliate_pg_hook.run(rawsql)
    # df.to_sql(commission_summary_table, con=engine_affiliate, if_exists='append', index=False)


def saveCommissionTable(df, datefrom, dateto, uuids):
    df['uuid'] = uuids
    df['from_transaction_date'] = datefrom
    df['to_transaction_date'] = dateto
    df = df.rename(columns={'affiliate_name': 'login_name'})

    # delete data
    rawsql =\
        """
        DELETE from {0}
        WHERE
            from_transaction_date = '{1}'
            AND to_transaction_date = '{2}'
            AND commission_status < {3}
        """.format(commission_table, datefrom, dateto, commission_status_approve)
    connAff = connectToDB(param_dic_aff)
    deleteData(connAff, rawsql, None)
    connAff = connectToDB(param_dic_aff)
    execute_values(connAff, df, commission_table)
    # conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    # engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

    # conn_affiliate_pg_hook.run(rawsql)
    # df.to_sql(commission_table, con=engine_affiliate, if_exists='append', index=False)


def get_file_path(prefix, path, yes):
    date = yes
    filename = prefix + '_' + date.strftime('%Y_%m_%d') + ".csv"
    path = path + prefix + '/' + \
        date.strftime('%Y') + '/' + date.strftime('%Y%m') + '/'
    return path + filename


def save_wager_summary_sqlite(df, frequency):
    if len(df) > 0:
        df = df.groupby(['username', 'product']).agg(
            {'stake': 'sum', 'count': 'sum', 'win_loss': 'sum'}).reset_index(drop=False)
        df = df.rename(columns={"username": "login_name"})
    else:
        df = pd.DataFrame(
            None, columns=['login_name', 'product', 'stake', 'count', 'win_loss'])

    if frequency == payout_frequency_monthly:
        sqlite_conn_temp_commission_monthly = sqlite3.connect(monthlydb_path)
        df.to_sql(name="wager_summary", con=sqlite_conn_temp_commission_monthly,
                  if_exists="append", index=False)

    elif frequency == payout_frequency_bi_monthly:
        sqlite_conn_temp_commission_bimonthly = sqlite3.connect(
            bimonthlydb_path)
        df.to_sql(name="wager_summary", con=sqlite_conn_temp_commission_bimonthly,
                  if_exists="append", index=False)

# for test


def GetRangeTime(arg_date, n):
    calculate_date = datetime.now()
    previous_yes = calculate_date - timedelta(days=arg_date+n+1)
    yes = calculate_date - timedelta(days=arg_date)
    previous_yes = datetime(
        previous_yes.year, previous_yes.month, previous_yes.day) + timedelta(hours=-8)
    yes = datetime(yes.year, yes.month, yes.day) + \
        timedelta(1) + timedelta(hours=-8)
    return previous_yes, yes


def group_data(df1, df2, dfMember):
    df = pd.concat([df1, df2])
    dfMember = dfMember.rename(columns={"member_name": "username"})
    df = df.merge(dfMember, how='inner', on='username')
    df = df.groupby(['username', 'product']).agg(
        {'stake': 'sum', 'count': 'sum', 'win_loss': 'sum'}).reset_index(drop=False)
    return df


def get_path_list(datefrom, dateto, prefix, path):
    all_filenames = []
    temp = datefrom
    while temp <= dateto + timedelta(days=1):
        if temp > datefrom:
            all_filenames.append(get_file_path(prefix, path, temp))
        temp = temp + timedelta(days=1)
    return all_filenames


def load_bti_data(date_from, date_to, dfMember, frequency):
    result = pd.DataFrame()
    all_filenames = get_path_list(date_from, date_to, 'bti', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for chunk in pd.read_csv(file, chunksize=chunksize):
                if len(chunk) > 0:
                    conditions = (
                        ((chunk.odds_style_of_user == 'European') & (chunk.odds_in_user_style >= EuroValue)) |
                        ((chunk.odds_style_of_user == 'Hongkong') & (chunk.odds_in_user_style >= HongkongValue)) |
                        ((chunk.odds_style_of_user == 'Malay') & (((chunk.odds_in_user_style >= -0.99) & (chunk.odds_in_user_style <= -0.1)) | ((chunk.odds_in_user_style >= MalayValue) & (chunk.odds_in_user_style <= 1)))) |
                        ((chunk.odds_style_of_user == 'Indo') & (((chunk.odds_in_user_style >= IndoValue) &
                                                                  (chunk.odds_in_user_style <= -0.1)) | ((chunk.odds_in_user_style >= 1) & (chunk.odds_in_user_style <= 9))))
                    )

                    chunk['eligible_stake'] = chunk['stake'].loc[conditions]
                    chunk.eligible_stake.fillna(0, inplace=True)
                    chunk.win_loss.fillna(0, inplace=True)
                    chunk['win_loss'] = chunk.apply(
                        lambda x: x['win_loss'] if x['eligible_stake'] > 0 else 0, axis=1)

                    chunk['count'] = 1
                    chunk['eligible_count'] = chunk.apply(
                        lambda x: 1 if x['eligible_stake'] > 0 else 0, axis=1)

                    chunk = chunk.loc[:, [
                        'username', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]
                    chunk["product"] = Btisports

                    chunk['create_at'] = pd.to_datetime(chunk['create_at'])
                    chunk['username'] = chunk['username'].astype(
                        str).str.lower()
                    result = group_data(result, chunk, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_tfgaming_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'tfgaming', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    conditions = ((
                        ((bet_data.member_odds_style == 'euro') & (bet_data.member_odds >= EuroValue)) |
                        ((bet_data.member_odds_style == 'hongkong') & (bet_data.member_odds >= HongkongValue)) |
                        ((bet_data.member_odds_style == 'malay') & (
                            ((bet_data.member_odds >= -0.99) & (bet_data.member_odds <= -0.1)) |
                            ((bet_data.member_odds >= MalayValue) & (bet_data.member_odds <= 1)))) |
                        ((bet_data.member_odds_style == 'indo') & (
                            ((bet_data.member_odds >= IndoValue) & (bet_data.member_odds <= -0.1)) |
                            ((bet_data.member_odds >= 1) & (bet_data.member_odds <= 9))))
                    )) & (bet_data.settlement_status == "settled")

                    bet_data['eligible_stake'] = bet_data['amount'].loc[conditions]
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.earnings.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['earnings'] if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"member_code": "username", "amount": "stake", "date_created": "create_at"})

                    bet_data = bet_data.loc[:, [
                        'username', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]
                    bet_data["product"] = TFgaming

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_sagaming_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'sagaming', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data['eligible_stake'] = bet_data.apply(
                        lambda x: x['rolling'] if x['rolling'] != 0 else 0, axis=1)
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.result_amount.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['result_amount'] if x['rolling'] != 0 else 0, axis=1)

                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"bet_amount": "stake", "bet_time": "create_at"})

                    bet_data = bet_data.loc[:, [
                        'username', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    bet_data["product"] = Sagaming

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_simpleplay_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'simpleplay', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data['eligible_stake'] = bet_data.apply(
                        lambda x: x['bet_amount'] if x['result_amount'] != 0 else 0, axis=1)
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.result_amount.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['result_amount'] if x['result_amount'] != 0 else 0, axis=1)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"bet_amount": "stake", "bet_time": "create_at"})

                    bet_data['product'] = bet_data.apply(
                        lambda x: SPSlot if 'SLOT' in x['detail'] else SPFish, axis=1)

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_allbet_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'allbet', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data['eligible_stake'] = bet_data['validamount']
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.winorloss.fillna(0, inplace=True)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"membercode": "username", "betamount": "stake", "bettime": "create_at", "winorloss": "win_loss"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]
                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_ebet_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(date_from, date_to, 'ebet', './data/wagers/')
    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data['eligible_stake'] = bet_data['validbet']
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    # todo
                    # bet_data.payout.fillna(0, inplace=True)
                    # bet_data['win_loss'] = bet_data.apply(
                    #     lambda x: x['payout'] - x['bet'] if x['validbet'] != 0 else 0, axis=1)
                    bet_data['win_loss'] = bet_data['validbet']
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['validbet'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"bet": "stake", "payouttimepartner": "create_at", "username": "username"})

                    bet_data = bet_data.loc[:, [
                        'username', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data["product"] = EBetgaming
                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_genesis_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'genesis', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data['eligible_stake'] = bet_data['validbet']
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.payout.fillna(0, inplace=True)
                    bet_data.bet.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['payout'] - x['bet'] if x['validbet'] != 0 else 0, axis=1)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['validbet'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"bet": "stake", "payouttimepartner": "create_at", "username": "username"})

                    bet_data = bet_data.loc[:, [
                        'username', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data["product"] = Genesis
                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_pgsoft_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'pgsoft', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"betamount": "stake", "bettime": "create_at", "membercode": "username"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_ag_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(date_from, date_to, 'ag', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.winamount.fillna(0, inplace=True)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"betamount": "stake", "placedatutc": "create_at", "membercode": "username", "winamount": 'win_loss'})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_bp_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(date_from, date_to, 'bp', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"transaction_time": "create_at", "membercode": "username", "winlost_amount": "win_loss"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_bp_sport_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'bpSport', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                bet_data = bet_data[bet_data['membercode'].notnull()]
                if len(bet_data) > 0:
                    conditions = (
                        ((bet_data.odds_type == 1) & (((bet_data.odds >= -0.99) & (bet_data.odds <= -0.1)) | ((bet_data.odds >= MalayValue) & (bet_data.odds <= 1)))) |
                        ((bet_data.odds_type == 2) & (bet_data.odds >= HongkongValue)) |
                        ((bet_data.odds_type == 3) & (bet_data.odds >= EuroValue)) |
                        ((bet_data.odds_type == 4) & (((bet_data.odds >= IndoValue) & (bet_data.odds <= -0.1)) | ((bet_data.odds >= 1) & (bet_data.odds <= 9)))) |
                        ((bet_data.odds_type == 5) & (((bet_data.odds >= UsValue) & (bet_data.odds <= -10))
                                                      | ((bet_data.odds >= 100) & (bet_data.odds <= 900))))
                    ) & ((bet_data.ticket_status != 'waiting') & (bet_data.ticket_status != 'running') & (bet_data.winlost_amount != 0))

                    bet_data['eligible_stake'] = bet_data['stake'].loc[conditions]
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.winlost_amount.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['winlost_amount'] if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"membercode": "username", "transaction_time": "create_at"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_sabacv_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'sabacv', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                bet_data = bet_data[bet_data['vendor_member_id'].notnull()]
                if len(bet_data) > 0:
                    conditions = (
                        ((bet_data.odds_type == 1) & (((bet_data.odds >= -0.99) & (bet_data.odds <= -0.1)) | ((bet_data.odds >= MalayValue) & (bet_data.odds <= 1)))) |
                        ((bet_data.odds_type == 2) & (bet_data.odds >= HongkongValue)) |
                        ((bet_data.odds_type == 3) & (bet_data.odds >= EuroValue)) |
                        ((bet_data.odds_type == 4) & (((bet_data.odds >= IndoValue) & (bet_data.odds <= -0.1)) | ((bet_data.odds >= 1) & (bet_data.odds <= 9)))) |
                        ((bet_data.odds_type == 5) & (((bet_data.odds >= UsValue) & (bet_data.odds <= -10))
                                                      | ((bet_data.odds >= 100) & (bet_data.odds <= 900))))
                    ) & ((bet_data.ticket_status != 'waiting') & (bet_data.ticket_status != 'running') & (bet_data.winlost_amount != 0))

                    bet_data['eligible_stake'] = bet_data['stake'].loc[conditions]
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data.winlost_amount.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['winlost_amount'] if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"vendor_member_id": "username", "transaction_time": "create_at"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_betradar_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'betradar', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data.ticketstake.fillna(0, inplace=True)
                    bet_data.totalreturn.fillna(0, inplace=True)

                    conditions = (
                        (bet_data.totalodds >= EuroValue) &
                        (bet_data.totalreturn != bet_data.ticketstake) &
                        (bet_data.status != 'placed')
                    )

                    bet_data['eligible_stake'] = bet_data['ticketstake'].loc[conditions]
                    bet_data.eligible_stake.fillna(0, inplace=True)

                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data['count'] = 1
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['totalreturn'] - x['eligible_stake'] if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"ticketstake": "stake", "receivedts": "create_at", "membercode": "username"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_evolution_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'evolution', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] != 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"placed_on": "create_at", "player_id": "username"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_digitain_data(date_from, date_to, dfMember, frequency):
    bet_data = pd.DataFrame()
    result = pd.DataFrame()
    all_filenames = get_path_list(
        date_from, date_to, 'digitain', './data/wagers/')

    for file in all_filenames:
        if os.path.exists(file):
            for bet_data in pd.read_csv(file, chunksize=chunksize):
                if len(bet_data) > 0:
                    conditions = (
                        (bet_data.odds >= EuroValue) |
                        (bet_data.odds >= HongkongValue) |
                        (((bet_data.odds >= -0.99) & (bet_data.odds <= -0.1)) | ((bet_data.odds >= MalayValue) & (bet_data.odds <= 1))) |
                        (((bet_data.odds >= IndoValue) & (bet_data.odds <= -0.1)) | ((bet_data.odds >= 1) & (bet_data.odds <= 9))) |
                        (bet_data.is_parlay == 1)
                    )

                    bet_data['eligible_stake'] = bet_data['amount'].loc[conditions]
                    bet_data.eligible_stake.fillna(0, inplace=True)
                    bet_data['win_loss'] = bet_data.apply(
                        lambda x: x['win_amount'] if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data['count'] = 1
                    bet_data['eligible_count'] = bet_data.apply(
                        lambda x: 1 if x['eligible_stake'] > 0 else 0, axis=1)

                    bet_data = bet_data.rename(
                        columns={"amount": "stake", "fill_date": "create_at"})

                    bet_data = bet_data.loc[:, [
                        'username', 'product', 'stake', 'count', 'eligible_stake', 'eligible_count', 'create_at', 'win_loss']]

                    bet_data['create_at'] = pd.to_datetime(
                        bet_data['create_at'])
                    bet_data['username'] = bet_data['username'].astype(
                        str).str.lower()
                    result = group_data(result, bet_data, dfMember)
    save_wager_summary_sqlite(result, frequency)


def load_bet_data(date_from, date_to, dfMember, frequency):
    start = time.time()
    load_bti_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_bti_data : ", end - start)

    start = time.time()
    load_sagaming_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_sagaming_data : ", end - start)

    start = time.time()
    load_simpleplay_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_simpleplay_data : ", end - start)

    start = time.time()
    load_evolution_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_evolution_data : ", end - start)

    start = time.time()
    load_tfgaming_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_tfgaming_data : ", end - start)

    start = time.time()
    load_allbet_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_allbet_data : ", end - start)

    start = time.time()
    load_ebet_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_ebet_data : ", end - start)

    start = time.time()
    load_pgsoft_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_pgsoft_data : ", end - start)

    start = time.time()
    load_betradar_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_bet_radar_data : ", end - start)

    start = time.time()
    load_ag_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_ag_data : ", end - start)

    start = time.time()
    load_bp_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_bp_data : ", end - start)

    start = time.time()
    load_bp_sport_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_bp_sport_data : ", end - start)

    start = time.time()
    load_sabacv_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_sabacv_data : ", end - start)

    start = time.time()
    load_genesis_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_genesis_data : ", end - start)

    load_digitain_data(date_from, date_to, dfMember, frequency)
    end = time.time()
    print("load_digitain_data : ", end - start)

    print("load_done")


def instant_calculate_monthly(**context):
    yy = datetime.now().year
    mm = datetime.now().month
    tt = get_current_tt()

    is_run_monthly = True
    is_run_bi_monthly = True

    if 'dag_run' in context and context['dag_run'].conf:

        tt_arg = context['dag_run'].conf['tt']
        if tt_arg > 0:
            is_run_monthly = False
            tt = tt_arg
        else:
            is_run_bi_monthly = False

        mm_arg = context['dag_run'].conf['mm']
        if mm_arg > 0:
            mm = mm_arg

        yy_arg = context['dag_run'].conf['yyyy']
        if yy_arg > 0:
            yy = yy_arg

    if is_run_monthly:
        datefrom, dateto = timerRangeMonthly(mm, yy)
        clean_sqlite_temp_commission(payout_frequency_monthly)
        instant_calculate(datefrom, dateto, payout_frequency_monthly)
    else:
        print("Do nothing")


def instant_calculate_bi_monthly(**context):
    yy = datetime.now().year
    mm = datetime.now().month
    tt = get_current_tt()

    is_run_monthly = True
    is_run_bi_monthly = True

    if 'dag_run' in context and context['dag_run'].conf:

        tt_arg = context['dag_run'].conf['tt']
        if tt_arg > 0:
            is_run_monthly = False
            tt = tt_arg
        else:
            is_run_bi_monthly = False

        mm_arg = context['dag_run'].conf['mm']
        if mm_arg > 0:
            mm = mm_arg

        yy_arg = context['dag_run'].conf['yyyy']
        if yy_arg > 0:
            yy = yy_arg

    if is_run_bi_monthly:
        datefrom, dateto = timerRangeBiMonthly(tt, mm, yy)
        clean_sqlite_temp_commission(payout_frequency_bi_monthly)
        instant_calculate(datefrom, dateto, payout_frequency_bi_monthly)
    else:
        print("Do nothing")


def instant_calculate(datefrom, dateto, frequency):
    uuids = str(uuid.uuid4())
    print(datefrom, dateto)

    # st1
    dfIdentity = extractIdentity()
    dfAffiliate = extractAffiliate(frequency)
    dfAI = dfAffiliate.loc[:, ['affiliate_id']]
    dfIdentity = dfIdentity.merge(dfAI, how='inner', on='affiliate_id')
    print("Done step 1")

    dfMember = dfIdentity.loc[:, ['member_name']]

    load_bet_data(datefrom, dateto, dfMember, frequency)

    print("Load bet done")

    preAffCommission = extractAffiliateCommission(datefrom, dateto, frequency)

    # setting
    extractAffiliateSetting()
    print("Done setting")

    # st2
    # commission fee
    dataDeposit = extractDeposit(datefrom, dateto, uuids, dfAI)
    dataWithdraw = extractWithdrawal(datefrom, dateto, uuids, dfAI)
    dataAdjustment = extractAdjustment(datefrom, dateto, uuids, dfAI)
    memberAffFee = calculate_member_aff_fee(
        dataDeposit, dataWithdraw, dataAdjustment)

    print("Done step 2")

    # st3
    # wager_summary tab product detail
    dfIden = dfIdentity.loc[:, ['affiliate_id',
                                'member_name', 'member_id', 'm_currency']]
    dfProduct = extractCollector(datefrom, dateto, dfIden, uuids, frequency)
    membeAffWager = calculate_member_aff_wager_product(dfProduct)

    print("Done step 3")

    # st4
    # tab summary detail
    dfIden = dfIdentity.loc[:, ['member_name', 'member_id', 'm_currency']]
    memberSummaryDetail = calculate_member_aff_commission_summary(
        memberAffFee, membeAffWager, dfIden, uuids, datefrom, dateto)

    print("Done step 4")

    # st5
    # summary
    dfIden = dfIdentity.loc[:, ['affiliate_id', 'member_name', 'member_id']]
    dfAffAdj = extractAffiliateAdjustment(datefrom, dateto)
    df = calculate_commission(dfIden, dfAffiliate, memberSummaryDetail,
                              dfAffAdj, datefrom, dateto, uuids, preAffCommission)

    print("Done step 5")


def timerRangeMonthly(mm, yy):
    firstday = datetime(year=yy, month=mm, day=1) + timedelta(hours=-8)
    lastday = datetime(year=yy, month=mm+1, day=1) + \
        timedelta(seconds=-1) + timedelta(hours=-8)
    return firstday, lastday


def timerRangeBiMonthly(tt, mm, yy):
    if tt == 1:
        firstday = datetime(year=yy, month=mm, day=1) + timedelta(hours=-8)
        lastday = datetime(year=yy, month=mm, day=16) + \
            timedelta(seconds=-1) + timedelta(hours=-8)
    else:
        firstday = datetime(year=yy, month=mm, day=16) + timedelta(hours=-8)
        lastday = datetime(year=yy, month=mm+1, day=1) + \
            timedelta(seconds=-1) + timedelta(hours=-8)
    return firstday, lastday


def get_current_tt():
    day = datetime.now().day
    if day <= 15:
        return 1
    return 2


if __name__ == '__main__':
    instant_calculate_monthly()
    instant_calculate_bi_monthly()


# commission_monthly_operator = PythonOperator(
#     task_id='commission_monthly', python_callable=instant_calculate_monthly, dag=dag)

# commission_bi_monthly_operator = PythonOperator(
#     task_id='commission_bi_monthly', python_callable=instant_calculate_bi_monthly, dag=dag)

# [commission_monthly_operator,commission_bi_monthly_operator]