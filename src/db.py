from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL
from models import Base

engine = create_engine(DATABASE_URL)
session_maker = sessionmaker(engine, expire_on_commit=False)


def create_db_and_tables():
    Base.metadata.create_all(engine)