import tweepy
import json
from pymongo import MongoClient


# conexão com o mongodb.
URL_CONNECTION = 'mongodb://localhost:27017/'
client = MongoClient(URL_CONNECTION)

# database
db = client['IWD2023']

# collection
collection = db['tweets']


class IWDStreamer(tweepy.Stream):

    def on_connect(self):
        # ao conectar
        print("Conectado a streaming API.")

    def on_error(self, status_code):
        print('Erro: ' + repr(status_code))
        return False

    def on_data(self, data):

        try:
            # carregando os dados recebidos e transformando em json
            tweet = json.loads(data)

            print(f"Criado em {tweet['created_at']}")

            print(
                f"https://twitter.com/{tweet['user']['screen_name']}/status/{tweet['id']}\n")

            # inserindo na base de dados
            collection.insert_one(tweet)

        except Exception as e:
            print(e)


# lendo os tokens
with open('tokens.json', 'r') as file:
    tokens = json.load(file)

streamer = IWDStreamer(
    consumer_key=tokens['api_key'],
    consumer_secret=tokens['api_secret_key'],
    access_token=tokens['access_token'],
    access_token_secret=tokens['access_token_secret']
)

# lendo os termos de busca 
with open('queries.txt', 'r') as file:
    words = file.readlines()
    print(f'Rastreando {len(words)} palavras chave\n')

# iniciando o stream
streamer.filter(track=words)
