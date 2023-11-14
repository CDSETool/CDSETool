import requests
import netrc
import threading
from datetime import datetime, timedelta

class Credentials:
    def __init__(self, username = None, password = None, token_endpoint = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"):
        self.__username = username
        self.__password = password
        self.__token_endpoint = token_endpoint

        self.__access_token = None
        self.__refresh_token = None
        self.__access_token_expires = None
        self.__refresh_token_expires = None

        self.__lock = threading.Lock()

        if self.__username == None or self.__password == None:
            self.__read_credentials()

    def get_session(self):
        self.__ensure_tokens()

        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {self.__access_token}"})
        return session

    def __exchange_credentials(self):
        data = {
            "grant_type": "password",
            "username": self.__username,
            "password": self.__password,
            "client_id": "cdse-public"
        }

        self.__token_exchange(data)

    def __refresh_access_token(self):
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.__refresh_token,
            "client_id": "cdse-public"
        }

        self.__token_exchange(data)

    def __token_exchange(self, data):
        response = requests.post(self.__token_endpoint, data=data)
        response.raise_for_status()
        response = response.json()

        self.__access_token = response["access_token"]
        self.__refresh_token = response["refresh_token"]
        self.__access_token_expires = datetime.now() + timedelta(seconds=response["expires_in"])
        self.__refresh_token_expires = datetime.now() + timedelta(seconds=response["refresh_expires_in"])

    def __ensure_tokens(self):
        with self.__lock:
            if self.__access_token == None:
                self.__exchange_credentials()

            if self.__access_token_expires < datetime.now():
                if self.__access_token_expires < datetime.now():
                    if self.__refresh_token_expires < datetime.now():
                        self.__exchange_credentials()
                    else:
                        self.__refresh_access_token()


    def __read_credentials(self):
        info = netrc.netrc()
        auth = info.authenticators(self.__token_endpoint)

        if auth:
            self.__username, _, self.__password = auth
        else:
            raise Exception("No credentials found")
