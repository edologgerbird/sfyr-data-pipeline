import time
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import json 

from TickerExtractor import TickerExtractor
from STIMovementExtractor import STIMovementExtractor

class SBRExtractor:
    def __init__(self):
        self.url = 'https://sbr.com.sg/stocks'
        self.req = None
        self.SBR_data_store = None

    def noOfPages(self):
        self.req = Request(self.url , headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(self.req).read()
        soup = BeautifulSoup(webpage, "lxml")
        noOfPages = soup.find('a',attrs={'title':'Go to last page'})['href'][6:]
        print('Total number of pages to scrape: ' + str(int(noOfPages)+1))
        return noOfPages
    
    def scrapeURL(self, url):
        self.req = Request(url , headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(self.req).read()

        time.sleep(0.5)  

        soup = BeautifulSoup(webpage, "lxml")
        metadata = soup.find('article')
        
        output_dict = dict()
        
        #Title
        title = metadata.find('h1',attrs={'class': 'nf__title'}).text.strip()
        output_dict['title'] = title
        
        #Text
        text = metadata.find('div',attrs={'class': 'nf__description'}).get_text(strip=True)
        clean_text = text.replace(u'\xa0', u' ')
        output_dict['text'] = clean_text
        
        #Date
        script = soup.find('script',attrs={'type': 'application/ld+json'}).text
        dictionary = json.loads(script)
        date = dictionary['@graph'][0]['datePublished']
        output_dict['date'] = date
        
        return output_dict
    
    def extract_SBR_data(self):
        print("Extracting SBR Data ...")
        articles_df = pd.DataFrame(columns = ['Title', 'Text', 'Link', 'Date'])
        noOfPages = self.noOfPages()

        for page in range(0,int(2)+1):
        #for page in range(0,int(noOfPages)):
            print('Processing page : ' + str(page+1) + ' out of ' + str(int(noOfPages)+1))

            url_page = self.url + '?page=' + str(page)
            self.req = Request(url_page , headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(self.req).read()
            
            time.sleep(0.5)
            
            soup = BeautifulSoup(webpage, "lxml")
            soup = soup.find('main',attrs={'class':'main-content'})
            links=soup.find_all('div',attrs={'class':'item with-border-bottom'})
        
            for j in links:
                link = j.find("h2",attrs={'class':'item__title size-24'}).find('a')['href'].strip()
                output_dict = self.scrapeURL(link)
                articles_df = articles_df.append({'Link' : link, 'Title': output_dict['title'], 'Text': output_dict['text'], 'Date': output_dict['date']}, ignore_index = True)
        
        self.SBR_data_store = articles_df
        print("SBR Data successfully extracted and populated")

    def SBR_data_postprocessing(self):
        print('Post processing of SBR data extracted...')
        te = TickerExtractor()
        ticker = te.populate_ticker_occurences(self.SBR_data_store.Text) #Tickers

        sme = STIMovementExtractor()
        sti_movement = sme.populate_sti_movement(self.SBR_data_store.Title) #Direction and percentage

        new_df = pd.concat([self.SBR_data_store, ticker, sti_movement],axis=1)
        new_df = new_df.loc[:,~new_df.columns.duplicated()]
        self.SBR_data_store = new_df
        print('Post processing of SBR data completed')
        
    def SBR_data_to_csv(self):
        self.SBR_data_store.to_csv('SBR_data_stocks.csv', index=False)
        print("SBR Data successfully saved to CSV")
        
    def load_SBR_data_from_source(self):
        self.extract_SBR_data()
        self.SBR_data_postprocessing()
        self.SBR_data_to_csv()
