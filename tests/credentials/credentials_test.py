"""Tests for CDSETool's credentials module."""

# pyright: reportAttributeAccessIssue=false

import datetime
from typing import Any

import pytest
import requests

from cdsetool.credentials import (
    Credentials,
    InvalidCredentialsException,
    NoCredentialsException,
    TokenExchangeException,
)

from ..mock_auth import mock_jwks, mock_openid, mock_token


def test_ensure_tokens(requests_mock: Any, mocker: Any) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

    credentials = Credentials("username", "password")
    assert credentials._Credentials__access_token is not None
    assert credentials._Credentials__refresh_token is not None

    credentials._Credentials__access_token_expires = (
        datetime.datetime.now() - datetime.timedelta(hours=100)
    )
    spy = mocker.spy(credentials, "_Credentials__token_exchange")
    credentials._Credentials__ensure_tokens()
    spy.assert_called_once()

    credentials._Credentials__access_token_expires = (
        datetime.datetime.now() - datetime.timedelta(hours=100)
    )
    credentials._Credentials__refresh_token_expires = (
        datetime.datetime.now() - datetime.timedelta(hours=100)
    )
    credentials._Credentials__ensure_tokens()
    assert spy.call_count == 2


def test_read_credentials(requests_mock: Any, mocker: Any) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

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


def test_refresh_token(requests_mock: Any, mocker: Any) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

    credentials = Credentials("username", "password")
    assert credentials._Credentials__access_token is not None
    assert credentials._Credentials__refresh_token is not None

    mock_token(requests_mock)  # mock again to return a new token

    prev_access_token = credentials._Credentials__access_token
    credentials._Credentials__access_token_expires = datetime.datetime.now()
    credentials._Credentials__refresh_token_expires = datetime.datetime.now()
    credentials._Credentials__ensure_tokens()

    assert credentials._Credentials__access_token is not None
    assert credentials._Credentials__refresh_token is not None
    assert prev_access_token != credentials._Credentials__access_token


def test_get_session(requests_mock: Any, mocker: Any) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

    credentials = Credentials("username", "password")
    session = credentials.get_session()

    assert isinstance(session, requests.Session)
    assert (
        session.headers.get("Authorization")
        == f"Bearer {credentials._Credentials__access_token}"
    )


def test_token_exchange(requests_mock: Any, mocker: Any) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

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
