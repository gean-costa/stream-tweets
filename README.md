# stream-tweets
Script para fazer a coleta em real time de tweets e salvá-los no MongoDB

### Instruções
Instalar as bibliotecas contidas no arquivo `requirements.txt`:
```
python -m pip install -r requirements.txt
```

Incluir no arquivo `tokens.json` os tokens de acesso da API do Twitter:
```
{
    "access_token" : "",
    "access_token_secret" : "",
    "api_key" : "",
    "api_secret_key" : ""
}
```

Incluir os termos de busca no arquivo `queries.txt`, onde cada linha corresponde a um termo de busca. Exemplo:
```
python
java
javascript
php
```

excutar o script `get_real_time_tweets.py`:
```
python get_real_time_tweets.py
```

**PS**: o banco de dados está configurado para o localhost. Caso o seu banco de dados esteja localizado em um outro ambiente, você terá que editar o script para que o projeto esteja na configuração correta.
