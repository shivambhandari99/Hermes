import os
import sys
from dotenv import load_dotenv

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect


load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

connection_string = f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'


engine = create_engine(connection_string, echo=False)
inspector = inspect(engine)

Session = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()
session = Session()
meta = MetaData()
meta.reflect(bind=engine)


def get_table_names_from_schema(schema):
    return engine.table_names(schema=schema)


def get_table_object(table_name, schema):
    try:
        return Table(table_name, meta, autoload=True, autoload_with=engine, schema=schema)

    except NoSuchTableError:
        return None


def create_table(table_obj):
    try:
        table_obj.create(bind=engine)
        return

    except Exception as e:
        print(e)
        sys.exit(-1)


def drop_table(table_obj):
    try:
        table_obj.drop(bind=engine, checkfirst=True)
        return

    except Exception as e:
        print(e)
        sys.exit(-1)

