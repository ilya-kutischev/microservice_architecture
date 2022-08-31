import time
from typing import Dict

import jwt
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
JWT_SECRET = os.environ['JWT_SECRET']
JWT_ALGORITHM = "HS256"


def token_response(token: str):
    return {
        "access_token": token
    }


def signJWT(email: str) -> Dict[str, str]:
    payload = {
        "email": email,
        "expires": time.time() + 60**60
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    return token_response(token)


def decodeJWT(token: str) -> dict:
    try:
        decoded_token = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return decoded_token if decoded_token["expires"] >= time.time() else None
    except:
        return {}