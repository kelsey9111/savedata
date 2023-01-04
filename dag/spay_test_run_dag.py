from time import sleep
from selenium.webdriver.common.by import By
from selenium import webdriver
from PIL import Image
from io import BytesIO
import sys
import os
import ast
from twocaptcha import TwoCaptcha
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import  datetime
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
captcha_key = '8ef519fed5b6f4b6126701606523f885'


vcb_link = 'https://vcbdigibank.vietcombank.com.vn/'
vcb_lin_stm = 'https://vcbdigibank.vietcombank.com.vn/thongtintaikhoan/chitiettaikhoan'

username = '0886330802'
passw =  'Cbn10646!'
localStoragestr = """"""

vcb_link = "https://vcbdigibank.vietcombank.com.vn/"
prefs = { "download.default_directory" : "data/excel" }
running_status = 2
normal_status = 1
VCB_code = 'VCB'


def save_captcha_img(driver):
    element = driver.find_element(By.CLASS_NAME, 'captcha')
    screen = driver.get_screenshot_as_png()

    location = element.location
    size = element.size

    im = Image.open(BytesIO(screen))

    screensize = (driver.execute_script("return document.body.clientWidth"),
                driver.execute_script("return window.innerHeight"))
    im = im.resize(screensize)

    left = location['x']
    top = location['y']
    right = (location['x'] + size['width'])
    bottom = (location['y'] + size['height'])

    k = im.crop((left, top, right, bottom))
    k.save('captcha.png')


def read_captcha_img():
    solver = TwoCaptcha(captcha_key)
    code = ""
    try:
        result = solver.normal("captcha.png")
    except Exception as e:
        sys.exit(e)
    else:
        code = result['code']

    return code
    

def instance(driver):
    global localStoragestr
    if localStoragestr == "":
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.ID, "app_password_login").send_keys(passw)
        # save_captcha_img(driver)
        # code = read_captcha_img()
        sleep(10)
        # driver.find_element(By.XPATH, '//*[@formcontrolname="captcha"]').send_keys(code)
        # driver.find_element(By.ID, "btnLogin").click()

        sleep(3)
        localStorage = driver.execute_script("return window.sessionStorage;")

        sleep(3)
        localStoragestr = str(localStorage)
        # print(localStoragestr)

    else:
        localStorage_str_dic = ast.literal_eval(localStoragestr)
        for k, v in localStorage_str_dic.items():
            driver.execute_script("return sessionStorage.setItem('" + str(k) + "', '" + str(v) + "');")

    #reload
    # driver.get(vcb_link)
    sleep(2)


    #acc stm
    at = driver.find_elements(By.CLASS_NAME, "ubg-white-2")
    if len(at)>=3:
        at[2].click()

    o = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH,"//option[text()='30 ngày gần nhất']")))
    o.click()

    k = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,"//li[text()='30 ngày gần nhất']")))
    k.click()

    WebDriverWait(driver, 15).until(EC.invisibility_of_element_located((By.CLASS_NAME, "vs-loading__load")))

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,"""//*[@title="30 ngày gần nhất"]""")))
    l = driver.find_element(By.XPATH,"//span[text()='Xuất excel']")
    l.click()

    print("done")

    # driver.quit()

    # #list bank page 1
    # driver.find_element(By.NAME, "next")

    sleep(1000)



def instance2():
    # try:
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(vcb_link)
        instance(driver)
    # except:
    #     driver.quit()

param_dic_wag = {
    'host'      : 'localhost',
    'database'  : 'spaydb',
    'user'      : 'postgres',
    'password'  : 'secret'
}

# url_object = URL.create(
#     "postgresql+pg8000",
#     username="postgres",
#     password="secret",  
#     host="localhost",
#     database="spaydb",
# )

def connectToDB(param_dic):
    conn = None
    try:
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn

def getData(conn, select_query,data, column_names):
    cursor = conn.cursor()
    try:
        cursor.execute(select_query,data)
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        cursor.close()
        return 1

    tupples = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(tupples, columns=column_names)
    return df

def deleteData(conn,delete_query,data):
    cursor = conn.cursor()
    try:
        cursor.execute(delete_query,data)
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

def updateTable(conn,sql_update_query, data):
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
        last_inserted_id = res[0]

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        conn.rollback()
        cursor.close()

        return 1
    return res

def load_to_postgres(username,bank_account_id,latest_ref_code):
      latest_ref_code_new = ''
      file_path = r'data/excel/{username}/Vietcombank_Account_Statement.xlsx'.format(username=username)
      if os.path.exists(file_path) == False:
        return 
      all_data_df = pd.read_excel(file_path, skiprows=12, skipfooter=15, usecols=[2, 3, 4, 5, 6])
      data_df = pd.DataFrame(columns=['transaction_date', 'description', 'ref_code', 'amount'])

      for index, row in all_data_df.iterrows():
        ref_code = row[0].split("\n")[1]
        transaction_date = row[0].split("\n")[0]
        if index == 0: 
            latest_ref_code_new = ref_code
       
         #todo set today
        # if transaction_date != "10/12/2022" or ref_code == latest_ref_code:
        #     break
        if ref_code == latest_ref_code:
                break


        description =  row[4]
        amount = str(row[2])
        
        if amount == 'nan':
            amount = float(str(row[1]).replace(",", ""))* (-1)
        else:
            amount = float(str(row[2]).replace(",", ""))


        new_row = {
            'transaction_date': [datetime.strptime(transaction_date,'%d/%m/%Y').date()],
            'description': [description],
            'ref_code': [ref_code],
            'amount': [float(amount)]
        }

        all_data_df = pd.DataFrame(new_row)
        data_df = pd.concat([data_df, all_data_df])

      data_df['bank_account_id'] = bank_account_id  
      save_data_db(data_df, latest_ref_code_new, bank_account_id) 


def save_data_db(df, latest_ref_code_new, bank_account_id):
    engine = create_engine("postgresql://postgres:tiger@localhost/spaydb")
    if len(df) > 0:  
        df.to_sql(name='vcb_{bank_account_id}_temp_table'.format(bank_account_id=bank_account_id), con=engine, if_exists='replace')
        with engine.begin() as cn:
            sql = """
                INSERT INTO bank_account_statement(transaction_date, description, ref_code, amount, bank_account_id)
                SELECT t.transaction_date, t.description, t.ref_code, t.amount, t.bank_account_id
                FROM vcb_{bank_account_id}_temp_table t
                WHERE NOT EXISTS
                    (SELECT 1 FROM bank_account_statement f
                        WHERE t.ref_code = f.ref_code
                        AND t.bank_account_id = f.bank_account_id);
                DROP TABLE vcb_{bank_account_id}_temp_table
                """.format(bank_account_id=bank_account_id)
            cn.execute(sql)

        update_bank_account(latest_ref_code_new, bank_account_id, normal_status)


def update_bank_account(latest_ref_code_new, bank_account_id, status):
    rawsql = "UPDATE bank_account SET latest_ref_code = '{0}', status = {1} WHERE id = {2}".format(latest_ref_code_new, status, bank_account_id)
    print(rawsql)
    conn = connectToDB(param_dic_wag)
    updateTable(conn,rawsql,None)

    # pg_hook = PostgresHook(postgres_conn_id="spay_conn_id")
    # pg_hook.run(sql)

# load_to_postgres('0886330802', 1, '5078 - 70d')
instance2()

