import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
import time
from datetime import datetime
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import time
from lxml import html
from random import randint
from threading import Thread
from multiprocessing import Process

proxy = "https://p.webshare.io:20000"

df = pd.DataFrame(columns=[0])

def scrapingFunc(start, end, proxy, df):
    for i in range(start, end):
        print("player: ", i, end=' ')  
        url_i = "https://www.transfermarkt.com/danilo-goiano/profil/spieler/" + str(i)

        # Initialize Beautiful Soup Objects
        headers = {"User-Agent":"Mozilla/5.0"}
        result = requests.get(url_i, headers=headers, proxies={'https': proxy})
        src = result.content
        soup = BeautifulSoup(src, 'lxml')

        ## print if the status code to see if we got blocked
        print('status code: ', result.status_code, end=' ')

        # Find the Name of the player
        try:
            name = soup.find('h1').getText()
        except:
            name = np.nan

        # Find the League of the player
        try:
            league = soup.find('span', class_="mediumpunkt").getText().replace(' ','').replace('\t','').replace('\n','')
            if league == "Lastposition:":
                league = np.nan
        except:
            league = np.nan

        # Find the Max Value of the player
        try:
            max_value = soup.find_all("div", class_="right-td")[-1].getText().replace(' ','').split('\n')[1]
        except:
            max_value = np.nan

        # Get everything we can from the Html table
        df_i = pd.read_html(str(src))[0]

        # Append Name and Value to the previously created datatable
        df_i = df_i.replace(r'\\n','', regex=True)
        df_i = df_i.append([['Full Name',name],['Max Value',max_value], ['League',league],['PlayerID',i]])

        # Merge the new table [df_i] to the master table [df]
        df = pd.merge(df, df_i, on=0, how='outer')

        # Create Url


        # Wait
        print("... Player loaded")
        sec = randint(8,18)
        print(f"waiting {sec} seconds...", end= ' ')
        time.sleep(sec)
        print("Done.")

        ## repeat again... if it works :)


df = pd.DataFrame(columns=[0])

if __name__ == '__main__':
    df_1 = Process(target = scrapingFunc, args=(1,100,"https://p.webshare.io:20000", df)).start()
    df_2 = Process(target = scrapingFunc, args=(100,200,"https://p.webshare.io:20001", df)).start()    



df_1.head()