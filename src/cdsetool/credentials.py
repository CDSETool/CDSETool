"""
This module provides a class for handling credentials for
the Copernicus Identity and Access Management (IAM) system.
"""

from datetime import datetime, timedelta
import netrc
import threading
from typing import Any, Dict, List, Union

import requests
import jwt
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class NoCredentialsException(Exception):
    """
    Raised when no credentials are found
    """


class InvalidCredentialsException(Exception):
    """
    Raised when credentials are invalid
    """


class DeprecatedNoTokenException(Exception):
    """
    Deprecated
    """


def NoTokenException(*args, **kwargs):  # pylint: disable=invalid-name
    """
    Raised when no token is available
    """
    from warnings import warn  # pylint: disable=import-outside-toplevel

    error_msg = [
        "Warning! NoTokenException is deprecated, and will be removed in"
        "the next major release."
    ]
    warn(" ".join(error_msg))
    return DeprecatedNoTokenException(*args, **kwargs)


class TokenExchangeException(Exception):
    """
    Raised when token exchange fails
    """


class TokenClientConnectionError(Exception):
    """
    Raised when token connection fails.
    """


class TokenExpiredSignatureError(Exception):
    """
    Raised when token signature has expired.
    """


class Credentials:  # pylint: disable=too-few-public-methods disable=too-many-instance-attributes
    """
    A class for handling credentials for the Copernicus Identity
    and Access Management (IAM) system
    """

    RETRY_CODES = frozenset([413, 429, 500, 502, 503])

    RETRIES = Retry(
        total=5,
        backoff_factor=0.5,
        raise_on_status=False,
        status_forcelist=RETRY_CODES,
    )

    def __init__(
        self,
        username: Union[str, None] = None,
        password: Union[str, None] = None,
        openid_configuration_endpoint: Union[str, None] = None,
        proxies: Union[Dict[str, str], None] = None,
    ) -> None:
        self.__username: Union[str, None] = username
        self.__password: Union[str, None] = password

        self.__proxies: Union[Dict[str, str], None] = proxies
        self.__openid_conf = None
        self.__jwks = None
        self.__openid_configuration_endpoint: str = (
            openid_configuration_endpoint
            or "https://identity.dataspace.copernicus.eu"
            + "/auth/realms/CDSE/.well-known/openid-configuration"
        )

        self.__access_token: Union[str, None] = None
        self.__refresh_token: Union[str, None] = None
        self.__access_token_expires: datetime = datetime.now() - timedelta(hours=8)
        self.__refresh_token_expires: datetime = self.__access_token_expires

        self.__lock = threading.Lock()

        if self.__username is None or self.__password is None:
            self.__read_credentials()

        self.__ensure_tokens()

    def get_session(self) -> requests.Session:
        """
        Returns a session with the credentials set as the Authorization header
        """
        return self.make_session(self, True, self.RETRIES, self.__proxies)

    @staticmethod
    def make_session(
        caller,
        authorization: bool,
        max_retries: Retry,
        proxies: Union[Dict[str, str], None],
    ) -> requests.Session:
        """
        Creates a new session. Authorization is only available from callers
        that are subclasses of Credentials.
        """
        if authorization:
            caller.__ensure_tokens()  # pylint: disable=protected-access

        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=max_retries))
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        if proxies is not None:
            session.proxies.update(proxies)
        if authorization:
            token = caller.__access_token  # pylint: disable=protected-access
            session.headers.update({"Authorization": f"Bearer {token}"})
        return session

    def __token_exchange(self, data: Dict[str, str]) -> timedelta:
        # Make a session that will retry post, respecting the retry-after
        # header when we get a 503 and a few other temporary failures.
        session = self.make_session(
            caller=self,
            authorization=False,
            max_retries=Retry(
                total=15,
                backoff_factor=0.5,
                allowed_methods=None,
                raise_on_status=False,
                status_forcelist=self.RETRY_CODES,
            ),
            proxies=self.__proxies,
        )
        response = session.post(self.__token_endpoint, data=data, timeout=120)

        if response.status_code == 401:
            raise InvalidCredentialsException(
                "Unable to exchange token with "
                + f"username: {self.__username} and "
                + f"password: {len(self.__password or '') * '*'}"
            )

        if response.status_code != 200:
            raise TokenExchangeException(f"Token exchange failed: {response.text}")

        response = response.json()

        self.__access_token = response["access_token"]
        self.__refresh_token = response["refresh_token"]
        return timedelta(seconds=response["refresh_expires_in"])

    def __ensure_tokens(self) -> None:
        with self.__lock:
            refresh_expire_delta = None
            if self.__access_token_expires < datetime.now():
                if self.__refresh_token_expires < datetime.now():
                    data = {
                        "grant_type": "password",
                        "username": self.__username,
                        "password": self.__password,
                        "client_id": "cdse-public",
                    }
                else:
                    data = {
                        "grant_type": "refresh_token",
                        "refresh_token": self.__refresh_token,
                        "client_id": "cdse-public",
                    }
                refresh_expire_delta = self.__token_exchange(data)
            if not self.__access_token:
                raise InvalidCredentialsException(
                    "Internal error: access token not available"
                )
            try:
                key = self.__jwk_client.get_signing_key_from_jwt(self.__access_token)
            except jwt.PyJWKClientConnectionError as e:
                raise TokenClientConnectionError from e
            try:
                data = jwt.decode(
                    self.__access_token,
                    key=key.key,
                    algorithms=self.__id_token_signing_algos,  # pylint: disable=protected-access
                    options={"verify_aud": False},
                )
            except jwt.ExpiredSignatureError as e:
                raise TokenExpiredSignatureError from e
            if refresh_expire_delta is not None:
                self.__access_token_expires = datetime.fromtimestamp(data["exp"])
                self.__refresh_token_expires = (
                    datetime.fromtimestamp(data["iat"]) + refresh_expire_delta
                )

    def __read_credentials(self) -> None:
        rv = netrc.netrc().authenticators(self.__token_endpoint)
        if isinstance(rv, tuple):
            self.__username, _, self.__password = rv
        else:
            raise NoCredentialsException("No credentials found")

    @property
    def __openid_configuration(self) -> Dict[str, Any]:
        if self.__openid_conf:
            return self.__openid_conf

        session = self.make_session(
            caller=self,
            authorization=False,
            max_retries=self.RETRIES,
            proxies=self.__proxies,
        )
        response = session.get(self.__openid_configuration_endpoint, timeout=120)
        response.raise_for_status()
        self.__openid_conf = response.json()
        return self.__openid_conf

    @property
    def __token_endpoint(self) -> str:
        return self.__openid_configuration["token_endpoint"]

    @property
    def __jwks_uri(self) -> str:
        return self.__openid_configuration["jwks_uri"]

    @property
    def __id_token_signing_algos(self) -> List[str]:
        return self.__openid_configuration["id_token_signing_alg_values_supported"]

    @property
    def __jwk_client(self) -> jwt.PyJWKClient:
        if self.__jwks:
            return self.__jwks

        self.__jwks = jwt.PyJWKClient(self.__jwks_uri)

        return self.__jwks


def validate_credentials(
    username: Union[str, None] = None, password: Union[str, None] = None
) -> bool:
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
    except TokenExchangeException:
        return False
