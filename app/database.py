from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app import config

DATABASE_URI = config.DATABASE_URI

sql_flavor = DATABASE_URI.split(':')[0]

if sql_flavor in ['mysql+pymysql', 'postgresql']:
    engine = create_engine(DATABASE_URI)
elif sql_flavor == 'sqlite':
    engine = create_engine(DATABASE_URI, connect_args={"check_same_thread": False})
else:
    raise Exception('Unknown database type')

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
