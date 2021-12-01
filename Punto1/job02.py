import requests
from datetime import datetime
import boto3
import pandas as pd
from bs4 import BeautifulSoup

# El siguiente Script se encarga de descaragr, subir, procesar los datos antes de subirlos como csv a S3

# Fecha actual
date = datetime.now()
# El tiempo
s3 = boto3.client('s3')
key = f'headlines/raw/periodico=eltiempo/year={date.year}/month={date.month}/day={date.day}/eltiempo.html'
pwd = '/tmp/eltiempo.html'
s3.download_file('parcial3-david', key, pwd)

# El publimetro
key = f'headlines/raw/periodico=publimetro/year={date.year}/month={date.month}/day={date.day}/publimetro.html'
pwd = '/tmp/publimetro.html'
s3.download_file('parcial3-david', key, pwd)

 #scraping el tiempo
with open('/tmp/eltiempo.html', encoding="utf-8") as file:
    content = file.read()
    soupET = BeautifulSoup(content,'html.parser')

category = []; title = []; url = []

articleET = soupET.find_all('div', attrs={'class': 'article-details'})

for row in articleET:
    try:
        category.append(str(str(str(str(row.find_all('a', attrs={'class':'category'})).split('<')).split('>')).split(',')[2]).replace('"','').replace("'",""))
        title.append(str(str(str(row.find_all('a', attrs={'class':'title'})).split('<')).split('>')[1]).replace('"','').replace("'","").replace(', /a',''))
        url.append('https://www.eltiempo.com'+str(row.find_all('a', attrs={'class':'title'})).split('"')[3])
    except:
        pass

columns = {'titles': title, 'categories':category, 'urls':url}
df = pd.DataFrame(columns)
df.to_csv('/tmp/eltiempo.csv', index=False, encoding='utf-8', sep=';')

#Scraping publimetro
with open('/tmp/publimetro.html', encoding="utf-8") as file:
    content = file.read()
    soupES = BeautifulSoup(content,'html.parser')

category = []; title = []; url = []

articleES = soupES.find_all("article")

for row in articleES:
    try:
        category_aux = row.find('span')
        if category_aux == None:
            category_aux2 = t
        else:
            category_aux2 = category_aux.get_text()
            t = category_aux2
        category.append(category_aux2)
        title_aux = row.find('h3')
        if title_aux == None:
            title_aux = row.find('h2')
        title_aux2 = title_aux.get_text()
        title.append(title_aux2)
        url_aux = row.find('a') 
        url_aux = url_aux['href']
        if url_aux[0] == '/':
            url_aux =  "https://www.publimetro.co/"+str(url_aux)
        url.append(url_aux)
    except:
        pass

columns = {'titles': title, 'categories':category, 'urls':url}
df = pd.DataFrame(columns)
df.to_csv('/tmp/publimetro.csv', index=False, encoding='utf-8', sep=';')

# El tiempo
s3 = boto3.resource('s3')
key = f'headlines/final/periodico=eltiempo/year={date.year}/month={date.month}/day={date.day}/eltiempo.csv'
pwd = f'/tmp/eltiempo.csv'
s3.meta.client.upload_file(pwd, 'parcial3-david', key)

# publimetro
s3 = boto3.resource('s3')
key = f'headlines/final/periodico=publimetro/year={date.year}/month={date.month}/day={date.day}/publimetro.csv'
pwd = f'/tmp/publimetro.csv'
s3.meta.client.upload_file(pwd, 'parcial3-david', key)