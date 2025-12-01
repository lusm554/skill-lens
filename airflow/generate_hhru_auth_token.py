import os

import requests
from dotenv import load_dotenv

load_dotenv(".hhru_env")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

url = "https://hh.ru/oauth/token"
payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
}

response = requests.post(url, data=payload)

if response.status_code == 200:
    data = response.json()
    access_token = data["access_token"]
    print(f"Ваш токен доступа: {access_token}")
else:
    print("Ошибка получения токена:", response.status_code, response.text)
