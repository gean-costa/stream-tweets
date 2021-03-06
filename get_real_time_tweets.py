import tweepy
import json
from pymongo import MongoClient


# conexão com o mongodb.
# URL_CONNECTION = 'mongodb://localhost:27017/'
client = MongoClient(URL_CONNECTION)

# database
db = client['DATABASE_NAME']

# collection
collection = db['COLLECTION_NAME']


class StreamListener(tweepy.StreamListener):

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

# fazendo a autenticação OAuth
auth = tweepy.OAuthHandler(
    consumer_key=tokens['api_key'],
    consumer_secret=tokens['api_secret_key']
)
auth.set_access_token(
    key=tokens['access_token'],
    secret=tokens['access_token_secret']
)

# instanciando a classe que foi criada para gerenciar a conexão
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))

# criando stream
streamer = tweepy.Stream(auth=auth, listener=listener)

# lendo os termos de busca 
with open('queries.txt', 'r') as file:
    words = file.readlines()
    print(f'Rastreando {len(words)} palavras chave\n')

# iniciando o stream
streamer.filter(track=words)
