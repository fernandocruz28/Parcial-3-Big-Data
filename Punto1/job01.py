import requests
from datetime import datetime
import boto3

## El siguiente Script sube los archivos html de ambos periodicos a el bucketA a S3

# Fecha actual
date = datetime.now()
## El tiempo
s3 = boto3.resource('s3')
urls = ["https://www.eltiempo.com/", "https://www.publimetro.co/"]
headers = {"Accept": "application/json"}
response = requests.request("GET", urls[0], headers=headers)
pwd = '/tmp/eltiempo.html'
archivo = open(pwd,'w', encoding='utf-8')
archivo.write(str(response.text))
archivo.close()
key =  f'headlines/raw/periodico=eltiempo/year={date.year}/month={date.month}/day={date.day}/eltiempo.html'
s3.meta.client.upload_file(pwd, 'parcial3-david', key)

## Publimetro
response = requests.request("GET", urls[1], headers=headers)
pwd = '/tmp/publimetro.html'
archivo = open(pwd,'w', encoding='utf-8')
archivo.write(str(response.text))
archivo.close()
key =  f'headlines/raw/periodico=publimetro/year={date.year}/month={date.month}/day={date.day}/publimetro.html'
s3.meta.client.upload_file(pwd, 'parcial3-david', key)