from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import json 
import sys
import cchardet  #To make scrapping faster

class SBRExtractor:
    def __init__(self):
        with open('utils/serviceAccount.json', 'r') as jsonFile:
            self.cred = json.load(jsonFile)
        self.url = self.cred["dataSources"]["sbr_url"]
        self.req = None
        self.SBR_data_store = pd.DataFrame(columns = ['Title', 'Text', 'Link', 'Date'])
        self.start_date = None
        self.end_date = None

    def noOfPages(self):
        self.req = Request(self.url , headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(self.req).read()
        soup = BeautifulSoup(webpage, "lxml")
        noOfPages = soup.find('a',attrs={'title':'Go to last page'})['href'][6:]
        #print('Total number of webpages: ' + str(int(noOfPages)+1))
        return noOfPages
    
    def scrapeURL(self, url):
        self.req = Request(url , headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(self.req).read() 

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
    
    def extract_SBR_data(self, start_date, end_date):
        print("Extracting SBR Data ...")

        self.start_date = start_date
        self.end_date = end_date
        
        #If start date is later than end date, an exception will be raised
        if self.start_date > self.end_date:
            raise Exception("Start date is later than end date. Please enter a valid date range.")

        noOfPages = self.noOfPages()

        for page in range(0,int(noOfPages)+1):
            
            url_page = self.url + '?page=' + str(page)
            self.req = Request(url_page , headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(self.req).read()

            soup = BeautifulSoup(webpage, "lxml")
            soup = soup.find('main',attrs={'class':'main-content'})
            links = soup.find_all('div',attrs={'class':'item with-border-bottom'})

            breaker = False #To break out of nested loop if end date reached already
            for j in links:
                link = j.find("h2",attrs={'class':'item__title size-24'}).find('a')['href'].strip()
                output_dict = self.scrapeURL(link)
                
                if output_dict['date'] >= self.end_date:
                    continue
                    
                if output_dict['date'] >= self.start_date:
                    sys.stdout.write("Currently scrapping article of datetime: %s \r" % (output_dict['date']) )
                    sys.stdout.flush()
                    #print('Added: ' + output_dict['title'] + ' Date: ' + output_dict['date'])
                    self.SBR_data_store = self.SBR_data_store.append({'Link' : link, 'Title': output_dict['title'], 'Text': output_dict['text'], 'Date': output_dict['date']}, ignore_index = True)
                else:
                    breaker = True
                    print('\nAll articles until end date has been scraped')
                    break

            if breaker:
                break

        print("SBR Data successfully extracted and populated")
        
    def SBR_data_to_csv(self):
        self.SBR_data_store.to_csv('./csv_store/SBR_data_stocks.csv', index=False ,encoding='utf-8-sig')
        print("SBR Data successfully saved to CSV")
        
    def load_SBR_data_from_source(self, start_date, end_date):
        self.extract_SBR_data(start_date, end_date)
        self.SBR_data_to_csv()
