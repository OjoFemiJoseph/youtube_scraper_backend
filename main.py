from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import pandas as pd
from bs4 import BeautifulSoup as bs
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import smtplib, ssl
import io
import pika
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def click_5_times(elm):
    elm.send_keys(Keys.DOWN)
    elm.send_keys(Keys.DOWN)
    elm.send_keys(Keys.DOWN)
    elm.send_keys(Keys.DOWN)
    elm.send_keys(Keys.DOWN)
    
def mailer(filename,email,part):
    
    subject = "An email with attachment from Youtube Scraper"
    body = "This is an email with attachment sent from Python"
    sender_email = os.environ.get("email")
    receiver_email = email 
    password = os.environ.get("pw")

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    # message["Bcc"] = receiver_email  # Recommended for mass emails

    # Add body to email
    message.attach(MIMEText(body, "plain"))
    
    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

def get_like_dislike(tag,chrome):
    link = 'https://www.youtube.com' +tag.a['href']
    chrome.get(link)
    time.sleep(4)
    try:
        soup2 = bs(chrome.page_source,'lxml')
    except:
        time.sleep(5)
        soup2 = bs(chrome.page_source,'lxml')
    like = soup2.find('div',{'id':'segmented-like-button'}).text.split('\n')[0]
    if not like:
        like = 0
    dislike = soup2.find('div',{'id':'segmented-dislike-button'}).text.split('\n')[0]
    if not dislike:
        dislike= 0
    return like,dislike

url = os.environ.get("q")

params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel


def export_csv(df):
    global part
    with io.StringIO() as buffer:
        
        df.to_csv(buffer)
        part = MIMEBase("application", "octet-stream")
        part.set_payload(buffer.read())
        
        return part

def callback(ch, method, properties, body):
    print('i was called')
    data = []
    data.append(str(body))
    link ,email = data[0].split('[')[1].split(']')[0].split(',')
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), options=chrome_options)
    chrome.get(link)
    for i in range(1000):
        elm = chrome.find_element(By.TAG_NAME,'html')
    #     elm = driver.find_element_by_tag_name("html")
    #     elm.send_keys(Keys.DOWN)
        click_5_times(elm)

    #     html.send_keys(Keys.END)
    soup = bs(chrome.page_source,'lxml')
    each_tag = soup.find_all('ytd-rich-grid-media')
    dta = []
    for tag in each_tag:
        i = tag.text.replace('\n','').strip()
        temp = i.split('Now playing')
        duration = temp[0]
        temp2 = temp[1].split('•')
        title = temp2[0]
        views,day = temp2[1].split('views')
        like,dislike = get_like_dislike(tag,chrome)
        dta.append([duration,title,views,day,like,dislike])
    filename = link.split('/')[-2]
    df = pd.DataFrame(data=dta,columns=['duration','title','views','day','like','dislike'])
    filename = f'{filename}.csv'
    part = export_csv(df)
    mailer(filename,email,part)



if __name__ == '__main__':
    while True:
        try:
            channel.basic_consume(
            queue='youtube', on_message_callback=callback, auto_ack=True)

            channel.start_consuming()
        except:
            pass