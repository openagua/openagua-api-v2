from fastapi import Request, Depends, Security
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker
from app.database import Base
from app.deps import api_key_header
from app.config import config

from app.deps import authorized_user

database_uri = config.DATABASE_URI
engine = create_engine(database_uri, connect_args={"check_same_thread": False},
                       poolclass=StaticPool)
Base.metadata.create_all(bind=engine)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


def override_get_g(request: Request, source_id: int = 1, project_id: int = 0, api_key=Security(api_key_header),
                   db=Depends(override_get_db)):
    return None
