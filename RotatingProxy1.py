import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import time
from lxml import html
from random import randint

# initialise
df = pd.DataFrame(columns=[0])


def scrapingFunc(start, end, df):
    retries = 0
    starttime = time.perf_counter()
    
    for i in range(start, end):
        print(f"player: {'{0:03}'.format(i)}", end=' ')  
        url_i = "https://www.transfermarkt.com/danilo-goiano/profil/spieler/" + str(i)

        # Initialize Beautiful Soup Objects
        headers = {"User-Agent":"Mozilla/5.0"}
        # Try to get the website. If it fails, try again in 5 seconds, if it fails again, try again in 30 seconds.
        try:
            result = requests.get(url_i, headers=headers, proxies={'https': "https://p.webshare.io:19999"})
            if result.status_code == 403:
                print ("403 Error, waiting 5 minutes")
                time.sleep(300)
                result = requests.get(url_i, headers=headers, proxies={'https': "https://p.webshare.io:19999"})
        except:
            retries = retries + 1
            print(f'Error, Retrying in 5 Seconds, {retries} retries sofar.')
            time.sleep(5)
            try:
                result = requests.get(url_i, headers=headers, proxies={'https': "https://p.webshare.io:19999"})
            except:
                retries = retries + 1
                print(f'Another Error, Retrying in 30 Seconds, {retries} retries sofar.')
                time.sleep(30)
                result = requests.get(url_i, headers=headers, proxies={'https': "https://p.webshare.io:19999"})
        src = result.content
        soup = BeautifulSoup(src, 'lxml')

        # print if the status code to see if we got blocked
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

        # Wait
        print("... Player loaded.")
        time.sleep(0)

        ## repeat again... if it works :)
    
    endtime = time.perf_counter()
    print(f'There were {retries} Errors in this instance.')
    print(f'Scraping Function Finnished in {round(endtime-starttime,2)} seconds and gathered {end-start} players.')
    
    return df



if __name__ == '__main__':
    # Run the Function
    df_final = scrapingFunc(1, 10000, df)
    
    # Save raw output to Excel File
    df_final.to_excel('001_raw.xlsx')

    # Save Transformed output to Excel File
    df_t = df_final.transpose()
    headers = df_t.iloc[0]
    df_t  = pd.DataFrame(df_t.values[1:], columns=headers)
    df_t.to_excel('001_transformed.xlsx')





    



    
  

    


