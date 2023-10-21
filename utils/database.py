import os
import psycopg2
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv



db_endpoint = 'testdb.cd0ub2t5tipq.ap-northeast-2.rds.amazonaws.com:5432/imd'
db_user = 'postgres'
db_password = 'postgres1%21'

host_url = "http://127.0.0.1:8000/"


def db_engine_get():
    """
    database 접근을 위한 DB Engine 생성
    :return:
    """
    db_uri = f'postgresql+psycopg2://{db_user}:{db_password}@{db_endpoint}'
    db_engine = sqlalchemy.create_engine(url=db_uri)
    return db_engine


if __name__ == "__main__":
    pass