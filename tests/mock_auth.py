"""Helper functions for mocking CDSE authentication endpoints."""

import base64
import datetime
import json
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _mock_openid(requests_mock: Any) -> None:
    with open("tests/credentials/mock/openid-configuration.json") as f:
        requests_mock.get(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/.well-known/openid-configuration",
            text=f.read(),
        )


def _mock_token(requests_mock: Any) -> None:
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


def _mock_jwks(mocker: Any) -> None:
    class MockResponse:
        def __init__(self, json_data) -> None:
            self.json_data = json_data

        def __enter__(self) -> "MockResponse":
            return self

        def read(self) -> bytes:
            return json.dumps(self.json_data).encode("utf-8")

        def __exit__(
            self, exc_type: object, exc_value: object, traceback: object
        ) -> None:
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
