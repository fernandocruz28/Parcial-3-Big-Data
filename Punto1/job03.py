from datetime import datetime
import boto3

# El siguiente Script se encarga de descaragr, subir, procesar los datos antes de subirlos como csv a S3

# Fecha actual
date = datetime.now()
# El tiempo
s3 = boto3.client('s3')
key = f'headlines/final/periodico=eltiempo/year={date.year}/month={date.month}/day={date.day}/eltiempo.csv'
pwd = '/tmp/eltiempo.csv'
s3.download_file('parcial3-david', key, pwd)

# El publimetro
key = f'headlines/final/periodico=publimetro/year={date.year}/month={date.month}/day={date.day}/publimetro.csv'
pwd = '/tmp/publimetro.csv'
s3.download_file('parcial3-david', key, pwd)

# El tiempo
s3 = boto3.resource('s3')
key = f'news/raw/periodico=eltiempo/year={date.year}/month={date.month}/day={date.day}/eltiempo.csv'
pwd = f'/tmp/eltiempo.csv'
s3.meta.client.upload_file(pwd, 'parcial3-david', key)

# publimetro
s3 = boto3.resource('s3')
key = f'news/raw/periodico=publimetro/year={date.year}/month={date.month}/day={date.day}/publimetro.csv'
pwd = f'/tmp/publimetro.csv'
s3.meta.client.upload_file(pwd, 'parcial3-david', key)