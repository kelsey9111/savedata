
from email import message
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
import schedule
import time
from plyer import notification
from datetime import datetime

live_path = "https://immi.homeaffairs.gov.au/what-we-do/whm-program/status-of-country-caps"

import os
import smtplib
import imghdr
from email.message import EmailMessage

def sendEmail():
    EMAIL_ADDRESS = "kelseynguyen9111@gmail.com"
    EMAIL_PASSWORD = "hxrakbauhbagebst"

    contacts = ['loannguyen9111@gmail.com']

    msg = EmailMessage()
    msg['Subject'] = '462 SOS!'
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ', '.join(contacts)

    msg.set_content('open open')

    msg.add_alternative("""\
    <!DOCTYPE html>
    <html>
        <body>
            <h1 style="color:SlateGray;">CLICK HERE TO OPEN!</h1>
            <a href="https://online.immi.gov.au">https://online.immi.gov.au</a>
        </body>
    </html>
    """, subtype='html')
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)



count = 0

def main():
    print("-------- Update at: " + datetime.now().strftime("%H:%M:%S") +"---------")
    global count
    count = count + 1
    r = requests.get(live_path)
    soup = bs(r.content)

    tr_elements = soup.find('table').find_all('tr')
    td = tr_elements[len(tr_elements)-1]
    sibling = td.find(class_="label label-primary")
    if sibling == None:
        print("-------- SOS: " + datetime.now().strftime("%H:%M:%S") +"---------")
        sendEmail()
        notification.notify(
        title = "SOS",
        message = '''------open------ ''' + datetime.now().strftime("%H:%M:%S"),
        timeout = 560000
        )
    # else:
    #     print("-------- SOS: " + datetime.now().strftime("%H:%M:%S") +"---------")
    #     sendEmail()
    #     notification.notify(
    #     title = "Waiting....",
    #     message = '''-Suspended- ''' +  datetime.now().strftime("%H:%M:%S"),
    #     timeout = 60
    #     )


    
schedule.every(1).minute.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)




