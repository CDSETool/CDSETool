"""
This module provides a class for handling credentials for
the Copernicus Identity and Access Management (IAM) system.
"""

from datetime import datetime, timedelta
import netrc
import threading
import requests
import jwt


class NoCredentialsException(Exception):
    """
    Raised when no credentials are found
    """


class InvalidCredentialsException(Exception):
    """
    Raised when credentials are invalid
    """


class NoTokenException(Exception):
    """
    Raised when no token is available
    """


class TokenExchangeException(Exception):
    """
    Raised when token exchange fails
    """


class Credentials:  # pylint: disable=too-few-public-methods disable=too-many-instance-attributes
    """
    A class for handling credentials for the Copernicus Identity
    and Access Management (IAM) system
    """

    def __init__(
        self,
        username=None,
        password=None,
        openid_configuration_endpoint=None,
    ):
        self.__username = username
        self.__password = password

        self.__openid_conf = None
        self.__jwks = None
        self.__openid_configuration_endpoint = (
            openid_configuration_endpoint
            or "https://identity.dataspace.copernicus.eu"
            + "/auth/realms/CDSE/.well-known/openid-configuration"
        )

        self.__access_token = None
        self.__refresh_token = None
        self.__access_token_expires = None
        self.__refresh_token_expires = None

        self.__lock = threading.Lock()

        if self.__username is None or self.__password is None:
            self.__read_credentials()

        self.__ensure_tokens()

    def get_session(self):
        """
        Returns a session with the credentials set as the Authorization header
        """
        self.__ensure_tokens()

        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {self.__access_token}"})
        return session

    def __exchange_credentials(self):
        data = {
            "grant_type": "password",
            "username": self.__username,
            "password": self.__password,
            "client_id": "cdse-public",
        }

        self.__token_exchange(data)

    def __refresh_access_token(self):
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.__refresh_token,
            "client_id": "cdse-public",
        }

        self.__token_exchange(data)

    def __token_exchange(self, data):
        response = requests.post(self.__token_endpoint, data=data, timeout=120)
        now = datetime.now()

        if response.status_code == 401:
            raise InvalidCredentialsException(
                "Unable to exchange token with "
                + f"username: {self.__username} and "
                + f"password: {len(self.__password) * '*'}"
            )

        if response.status_code != 200:
            raise TokenExchangeException(f"Token exchange failed: {response.text}")

        response = response.json()

        self.__access_token = response["access_token"]
        self.__refresh_token = response["refresh_token"]
        self.__access_token_expires = now + timedelta(seconds=response["expires_in"])
        self.__refresh_token_expires = now + timedelta(
            seconds=response["refresh_expires_in"]
        )

    def __ensure_tokens(self):
        with self.__lock:
            if self.__access_token is None:
                self.__exchange_credentials()

            if self.__access_token_expires < datetime.now():
                if self.__refresh_token_expires < datetime.now():
                    self.__exchange_credentials()
                else:
                    self.__refresh_access_token()
            self.__validate_tokens()

    # validate __access_token and __refresh_token using the jwks certs
    def __validate_tokens(self):
        if self.__access_token is None:
            raise NoTokenException("No access token found")

        if self.__refresh_token is None:
            raise NoTokenException("No refresh token found")

        key = self.__jwk_client.get_signing_key_from_jwt(self.__access_token)
        jwt.decode(
            self.__access_token,
            key=key.key,
            algorithms=key._algorithms,  # pylint: disable=protected-access
            options={"verify_aud": False},
        )

    def __read_credentials(self):
        try:
            self.__username, _, self.__password = netrc.netrc().authenticators(
                self.__token_endpoint
            )
        except Exception as exc:
            raise NoCredentialsException("No credentials found") from exc

    @property
    def __openid_configuration(self):
        if self.__openid_conf:
            return self.__openid_conf

        response = requests.get(self.__openid_configuration_endpoint, timeout=120)
        response.raise_for_status()
        self.__openid_conf = response.json()
        return self.__openid_conf

    @property
    def __token_endpoint(self):
        return self.__openid_configuration["token_endpoint"]

    @property
    def __jwks_uri(self):
        return self.__openid_configuration["jwks_uri"]

    @property
    def __jwk_client(self):
        if self.__jwks:
            return self.__jwks

        self.__jwks = jwt.PyJWKClient(self.__jwks_uri)

        return self.__jwks


def validate_credentials(username=None, password=None):
    """
    This function validates CDSE credentials and returns a bool.
    If credentials are none, .netrc will be validated
    """
    try:
        Credentials(username, password)
        return True
    except NoCredentialsException:
        return False
    except InvalidCredentialsException:
        return False
    except NoTokenException:
        return False
    except TokenExchangeException:
        return False
