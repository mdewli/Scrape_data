""" All Libraries are here..."""
import datetime
from operator import index
import time
import pandas as pd
import psycopg2 as pg
import requests
from bs4 import BeautifulSoup
import warnings
import os
from sqlalchemy import create_engine


warnings.filterwarnings("ignore")
x = datetime.datetime.now()
n = x.strftime("__%b_%d_%Y")


"""Function that crawls all the events from the URL and returns a pandas dataframe which contains all data """
def get_data():
    data = {
            'Title': [],
            'Artist': [],
            'Work': [],
            'Image Link': [],
            'location': [],
            'date':[],
            'Time': [],
            }
    
    print('=================== Data Scraping ===================')
    """Main url"""
    url = "https://www.lucernefestival.ch/en/program/summer-festival-22"
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')

    try:
        """Find all Event sections """
        divs = soup.find('div', {'id': 'event-list'}).find_all('div', {'class': 'entry'})
        """Get each Event section """
        for div in divs:
            title = ''
            artist = ''
            work = ''
            image_link = ''
            location = ''
            date = ''
            times = ''
            """Get the Title of Event"""
            try:
                title = div.find('div', {'class': 'event-info'}).find('p', {'class': 'title'}).text.replace('\n', '')
            except:
                None
            """Get Work from Event"""
            try:
                work = div.find('div', {'class': 'event-info'}).find('span', {'class': 'sponsor'}).text.replace('\n', '')
            except:
                None
            """Get the name of the Artist from Event"""
            try:
                artist = div.find('div', {'class': 'event-info'}).find('p', {'class': 'subtitle'}).text.replace('\n', '').replace(work, '').replace('\t', '')
            except:
                None
            """Get the Image link from Event"""
            try:
                image_link = str(div.find('div', {'class': 'image'}).get('style')).replace('background: url(', '').replace(') center center;', '')
            except:
                None
            """Get the Location of Event"""
            try:
                location = div.find('div', {'class': 'date-place'}).find('p', {'class': 'location'}).find('a').text.replace('\n', '').replace('\t', '')
            except:
                None
            """Get the Date of Event"""
            try:
                date_1 = div.find('div', {'class': 'date-place'}).find('div', {'class': 'left'}).find('p', {'class': 'date'}).text.replace('\n', ' ')
                date_2 = div.find('div', {'class': 'date-place'}).find('div', {'class': 'left'}).find('p', {'class': 'month'}).text.replace('\n', ' ')
                date = str(date_1 + ' ' + date_2)
            except:
                None
            """Get the Time of Event"""
            try:
                time_1 = div.find('div', {'class': 'date-place'}).find('div', {'class': 'right'}).find('span', {'class': 'day'}).text.replace('\n', ' ')
                time_2 = div.find('div', {'class': 'date-place'}).find('div', {'class': 'right'}).find('span', {'class': 'time'}).text.replace('\n', ' ')
                times = str(time_1 + ' ' + time_2)
            except:
                None

            """append data to dictionary"""
            if title != '':
                data['title'].append(title)
                data['artist'].append(artist)
                data['work'].append(work)
                data['image_link'].append(image_link)
                data['location'].append(location)
                data['date'].append(date)
                data['time'].append(times)
          
    except:
        pass
    df = pd.DataFrame(data)
    df.to_csv('lucernefestival_data.csv', encoding='utf-8')
    
    print("Done")


    




def insert_to_db():
    '''Get data from csv file into a dataframe'''
    df = pd.read_csv('lucernefestival_data.csv', index_col=False)

    database_url = "postgresql://myuser:password@localhost:5433/events_data"

    conn = create_engine(database_url)

    conn.execute('DROP TABLE IF EXISTS artist_data')

    conn.execute('''
                CREATE TABLE artist_data(
                    title       TEXT,
                    artist      TEXT,
                    work        TEXT,
                    image_link  TEXT,
                    location    TEXT,
                    date        TEXT,
                    time        TEXT
                );
                ''')
    df.to_sql('artist_data',conn, if_exists='append',index=False)
    
    
if __name__ == '__main__':
    insert_to_db()
    print("Completed successfully..")

    