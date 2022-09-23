from fastapi.testclient import TestClient
# from authGateway.main import *
import pytest
# from authGateway.main import app
import requests
# client = TestClient(app)
import random

num = random.randint(1, 10 ** 10)


def test_read_main():
    response = requests.get("http://0.0.0.0:5000/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to our service"}

def test_signup(num = num):
    params = {
        "fullname": f"test{num}",
        "email": f"test{num}@example.com",
        "password": "testtest"
    }
    response = requests.post(
        "http://0.0.0.0:5000/user/signup",
        json=params
    )
    assert response.status_code == 200
    assert "access_token" in list(response.json().keys())


def test_login(num=num):
    params = {
        "email": f"test{num}@example.com",
        "password": "testtest"
    }
    response = requests.post(
        "http://0.0.0.0:5000/user/login",
        json=params
    )
    assert response.status_code == 200
    assert "access_token" in list(response.json().keys())

def test_add_data(num=num):
    params = {
        "email": f"test{num}@example.com",
        "password": "testtest"
    }
    response = requests.post(
        "http://0.0.0.0:5000/user/login",
        json=params
    )
    assert response.status_code == 200

    token = response.json()["access_token"]
    params = {
        "header": "Information",
        "data": "data..."
    }

    response = requests.post(
        f"http://0.0.0.0:5000/add_data?token={token}",
        json=params
    )