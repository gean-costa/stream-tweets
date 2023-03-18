from datetime import datetime
import numpy as np
import tweepy
import json
from pymongo import MongoClient, ASCENDING, DESCENDING
from multiprocessing import Pool



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

api = tweepy.API(auth, wait_on_rate_limit=True)

# configurações do mongoDB
MONGO_HOST = 'mongodb://localhost:27017/'
client = MongoClient(MONGO_HOST)
db = client['8M2022']

# configurando as collections de origem e destino
coll_from = db.tweets
coll_to = db.tweets_updated

print(f'########## Iniciando processo de atualização de tweets em {datetime.now()}')

per_page = 1000
project = { 'id': True, 'timestamp_ms': True}

last_id = list(coll_to.find({}, {'_id': True}).sort('_id', DESCENDING).limit(1))[0]['_id']

print(last_id)

ts = list(coll_from.find({'id': last_id}, {'timestamp_ms': 1, '_id': 0}))[0]['timestamp_ms']

print(ts)











# def updater(tweets):
#     updated_tweets = api.lookup_statuses(id=list(tweets), tweet_mode='extended')
#     updated_docs = [ut._json for ut in updated_tweets]
#     print(len(updated_docs))
#     coll_to.insert_many(updated_docs)


# filter = { 'timestamp_ms': { '$gt': ts } }
# docs = list(coll_from.find(filter, project).sort('timestamp_ms', ASCENDING).limit(per_page))

# tweets_ids = np.array([doc['id'] for doc in docs]).reshape((int(per_page/100), 100))

# with Pool(4) as worker:
#     worker.map(updater, tweets_ids)




# ts = docs[-1]['timestamp_ms']
# print(ts)

# filter = { 'timestamp_ms': { '$gt': ts } }
# docs = list(coll_from.find(filter, project).sort('timestamp_ms', ASCENDING).limit(per_page))

# ts = docs[-1]['timestamp_ms']
# print(ts)

# tweets_ids = [doc['id'] for doc in docs]
# updated_tweets = api.lookup_statuses(id=list(tweets_ids), tweet_mode='extended')
# updated_docs = [ut._json for ut in updated_tweets]
# print(updated_docs[0])




    

