from datetime import datetime
import numpy as np
import tweepy
import json
from pymongo import MongoClient, ASCENDING, errors, DESCENDING
import logging
from multiprocessing import Pool
import time
import random
import math


logging.basicConfig(
    filename='logs_update.txt',
    encoding='utf-8',
    format='[%(levelname)s][%(asctime)s]: %(message)s',
    datefmt='%d/%m/%Y %I:%M:%S %p',
    level=logging.INFO
)


def api():

    # lendo os tokens
    with open('tokens.json', 'r') as file:
        tokens = json.load(file)

    # autenticação
    auth = tweepy.OAuth1UserHandler(
        tokens['api_key'],
        tokens['api_secret_key'],
        tokens['access_token'],
        tokens['access_token_secret']
    )

    return tweepy.API(auth, wait_on_rate_limit=True)


def mongo(host='mongodb://localhost:27017/', database='8M2022'):
    # configurações do mongoDB
    client = MongoClient(host=host)
    
    return client[database]


api = api()
db = mongo()

# configurando as collections de origem e destino
coll_from = db.tweets
coll_to = db.tweets_updated


def update_insert(tweets):

    # requisição a API
    updated_tweets = api.lookup_statuses(
        id=list(tweets), tweet_mode='extended')
    
    time.sleep(random.choice([0.5, 1.0, 1.5, 2.0]))

    # organizando os dados retornados
    updated_docs = [ut._json for ut in updated_tweets]
    # adicionando o campo _id
    updated_docs = [dict(item, _id=item['id']) for item in updated_docs]

    logging.info(
        f'{len(tweets)} tweets enviados | {len(updated_docs)} tweets atualizados')
    print(
        f'{len(tweets)} tweets enviados | {len(updated_docs)} tweets atualizados')

    for new_doc in updated_docs:  # new_docs:
        try:
            # INSERINDO DOCUMENTOS ATUALIZADOS NA NOVA BASE
            coll_to.insert_one(new_doc)
        except errors.BulkWriteError as error:
            logging.info(f'BulkWriteError: {error.details}')
            print(f'ulkWriteError: {error.details}')
            pass
        except errors.DuplicateKeyError as error2:
            logging.info(f'DuplicateKeyError: {error2.details}')
            print(f'DuplicateKeyError: {error2.details}')
            pass

def reshaper(lista):
    if len(lista)>100:
        num_blocks = math.ceil(len(lista)/100)
        return [lista[(a-1)*100:a*100] for a in range(1,num_blocks+1)]
    return lista


logging.info(f'Iniciando processo de atualização de tweets')
print(
    f'########## Iniciando processo de atualização de tweets em {datetime.now()}')

last_id = list(coll_to.find({}, {'_id': True}).sort('_id', DESCENDING).limit(1))[0]['_id']
ts = list(coll_from.find({'id': last_id}, {'timestamp_ms': 1, '_id': 0}))[0]['timestamp_ms']
# ts = '1646784780469'

per_page = 10_000

num_docs = per_page
num_docs_queried = 0

project = {'id': True, 'timestamp_ms': True, '_id': False}

while num_docs > 0:

    # query
    filter = {'timestamp_ms': {'$gt': ts}}

    # executando a query
    docs = list(coll_from.find(filter, project).sort(
        'timestamp_ms', ASCENDING).limit(per_page))
    num_docs_queried += len(docs)
    num_docs = len(docs)

    if num_docs > 0:
        logging.info(
            f'{num_docs_queried} documentos selecionados')
        print(
            f'########## {num_docs_queried} documentos selecionados')

        ts = docs[-1]['timestamp_ms']
        tweets_ids = reshaper([doc['id'] for doc in docs])

        
        try:
            with Pool(8) as workers:
                workers.map(update_insert, tweets_ids)
        except:
            logging.info(f'Erro com o processo de atualização de tweets')
            print(f'Erro com o processo de atualização de tweets')
            num_docs = 0


logging.info(
    f'Finalizado processo de atualização de tweets')
print(
    f'########## Finalizado processo de atualização de tweets em {datetime.now()}')
