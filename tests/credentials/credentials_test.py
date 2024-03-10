import json
import pytest
import requests
import datetime
import jwt
import base64
from cryptography.hazmat.primitives.asymmetric import rsa

private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

from cdsetool.credentials import (
    Credentials,
    NoCredentialsException,
    InvalidCredentialsException,
    TokenExchangeException,
)


def _mock_openid(requests_mock):
    with open("tests/credentials/mock/openid-configuration.json") as f:
        requests_mock.get(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/.well-known/openid-configuration",
            text=f.read(),
        )


def _mock_token(requests_mock):
    headers = {"alg": "RS256", "typ": "JWT", "kid": "key-9000"}
    now = datetime.datetime.now()
    payload = {
        "exp": now.timestamp() + 3600,
        "iat": now.timestamp(),
        "jti": "bb4d3fab-1a39-442c-9a6f-072a167543c0",
        "iss": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE",
        "aud": ["CLOUDFERRO_PUBLIC", "account"],
        "sub": "bfb3df17-4506-4adf-86ab-9bb9d03f11f1",
        "typ": "Bearer",
        "azp": "cdse-public",
        "session_state": "4adfd9e9-27d2-496d-9c10-bd3a54c8f1a3",
        "allowed-origins": [
            "https://localhost:4200",
            "*",
            "https://workspace.staging-cdse-data-explorer.apps.staging.intra.cloudferro.com",
        ],
        "realm_access": {
            "roles": [
                "offline_access",
                "uma_authorization",
                "default-roles-cdas",
                "copernicus-general",
            ]
        },
        "resource_access": {
            "account": {
                "roles": ["manage-account", "manage-account-links", "view-profile"]
            }
        },
        "scope": "AUDIENCE_PUBLIC openid email profile user-context",
        "sid": "03a2986d-ccfa-45e7-8e3a-55982e7e2a6e",
        "group_membership": [
            "/access_groups/user_typology/copernicus_general",
            "/organizations/default-4f0080be-2b79-4837-b6a2-7f10c2b9ee1d/regular_user",
        ],
        "email_verified": True,
        "organizations": ["default-4f0080be-2b79-4837-b6a2-7f10c2b9ee1d"],
        "name": "User Full Name",
        "user_context_id": "b9ab6ae1-83b0-433d-828f-e9e06adcc4a2",
        "context_roles": {},
        "context_groups": [
            "/access_groups/user_typology/copernicus_general/",
            "/organizations/default-4f0080be-2b79-4837-b6a2-7f10c2b9ee1d/regular_user/",
        ],
        "preferred_username": "user@example.com",
        "given_name": "User first name",
        "user_context": "default-4f0080be-2b79-4837-b6a2-7f10c2b9ee1d",
        "family_name": "User last name",
        "email": "user@example.com",
    }

    response = json.dumps(
        {
            "access_token": jwt.encode(
                payload, private_key, algorithm="RS256", headers=headers
            ),
            "expires_in": 600,
            "refresh_expires_in": 3600,
            "refresh_token": "check-for-refresh-token-not-implementation-yet",
            "token_type": "Bearer",
            "not-before-policy": 0,
            "session_state": "4adfd9e9-27d2-496d-9c10-bd3a54c8f1a3",
            "scope": "AUDIENCE_PUBLIC openid email profile user-context",
        }
    )

    requests_mock.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        text=response,
    )


def _mock_jwks(mocker):
    class MockResponse:
        def __init__(self, json_data):
            self.json_data = json_data

        def __enter__(self):
            return self

        def read(self):
            return json.dumps(self.json_data).encode("utf-8")

        def __exit__(self, exc_type, exc_value, traceback):
            pass

    n = private_key.public_key().public_numbers().n
    e = private_key.public_key().public_numbers().e
    n = base64.urlsafe_b64encode(
        n.to_bytes((n.bit_length() + 7) // 8, byteorder="big")
    ).decode("utf-8")
    e = base64.urlsafe_b64encode(
        e.to_bytes((e.bit_length() + 7) // 8, byteorder="big")
    ).decode("utf-8")

    jwks = {
        "keys": [
            {
                "kid": "key-9000",
                "kty": "RSA",
                "alg": "RS256",
                "use": "sig",
                "n": n,
                "e": e,
            }
        ]
    }
    mocker.patch("urllib.request.urlopen", return_value=MockResponse(jwks))


def test_ensure_tokens(requests_mock, mocker):
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    credentials = Credentials("username", "password")
    assert credentials._Credentials__access_token is not None
    assert credentials._Credentials__refresh_token is not None

    credentials._Credentials__access_token_expires = (
        datetime.datetime.now() - datetime.timedelta(hours=100)
    )
    spy = mocker.spy(credentials, "_Credentials__refresh_access_token")
    credentials._Credentials__ensure_tokens()
    spy.assert_called_once()

    credentials._Credentials__access_token_expires = (
        datetime.datetime.now() - datetime.timedelta(hours=100)
    )
    credentials._Credentials__refresh_token_expires = (
        datetime.datetime.now() - datetime.timedelta(hours=100)
    )
    spy = mocker.spy(credentials, "_Credentials__exchange_credentials")
    credentials._Credentials__ensure_tokens()
    spy.assert_called_once()


def test_read_credentials(requests_mock, mocker):
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    mocker.patch(
        "netrc.netrc",
        return_value=mocker.Mock(
            authenticators=lambda x: ("username", None, "password")
        ),
    )

    credentials = Credentials()
    assert credentials._Credentials__username == "username"
    assert credentials._Credentials__password == "password"

    mocker.patch("netrc.netrc", return_value=mocker.Mock(authenticators=lambda x: None))

    with pytest.raises(NoCredentialsException):
        credentials = Credentials()


def test_refresh_token(requests_mock, mocker):
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    credentials = Credentials("username", "password")
    assert credentials._Credentials__access_token is not None
    assert credentials._Credentials__refresh_token is not None

    _mock_token(requests_mock)  # mock again to return a new token

    prev_access_token = credentials._Credentials__access_token
    credentials._Credentials__access_token = None
    credentials._Credentials__refresh_access_token()

    assert credentials._Credentials__access_token is not None
    assert credentials._Credentials__refresh_token is not None
    assert prev_access_token != credentials._Credentials__access_token


def test_get_session(requests_mock, mocker):
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    credentials = Credentials("username", "password")
    session = credentials.get_session()

    assert isinstance(session, requests.Session)
    assert (
        session.headers.get("Authorization")
        == f"Bearer {credentials._Credentials__access_token}"
    )


def test_token_exchange(requests_mock, mocker):
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    credentials = Credentials("myuser123123", "password")

    data = {
        "grant_type": "password",
        "username": credentials._Credentials__username,
        "password": credentials._Credentials__password,
        "client_id": "cdse-public",
    }

    credentials._Credentials__token_exchange(data)

    requests_mock.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        text="{}",
        status_code=401,
    )

    with pytest.raises(
        InvalidCredentialsException, match="with username: myuser123123"
    ):
        credentials._Credentials__token_exchange(data)

    requests_mock.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        text="Failure 123123",
        status_code=500,
    )

    with pytest.raises(TokenExchangeException, match="Failure 123123"):
        credentials._Credentials__token_exchange(data)
