import pytest
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
from app.main import app, api_prefix
from app.deps import get_db, get_g
from app.database import Base
from app import config

SQLALCHEMY_DATABASE_URL = "sqlite:///./test_auth.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool
)
Base.metadata.create_all(bind=engine)


def override_get_db():
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


def override_get_g():
    return None


app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[get_g] = override_get_g

client = TestClient(app, headers={'X-API-KEY': config.TEST_API_KEY})
client.base_url = f'{client.base_url}{api_prefix}'


def test_login():
    resp = client.post('auth/login')
    assert resp.status_code == 200


def test_register_user():
    form_data = {
        'username': 'herr.rhein_gmail.com',
        'password': 'testpassword',
        'origin': 'http://127.0.0.1:8000/register'
    }

    # poorly formatted email
    resp = client.post('auth/register', data=form_data)
    assert resp.status_code == 422

    # correct email, no current user
    form_data.update(username=form_data['username'].replace('_', '@'))
    resp = client.post('auth/register', data=form_data)
    assert resp.status_code == 200

    # current user exists
    resp = client.post('auth/register', data=form_data)
    assert resp.status_code == 409  # already exists
