from typing import Dict, List, Union

from oltp_app.models import Model
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker


class PostgresSQLClient:
    def __init__(self, database, user, password, host="127.0.0.1", port="5432"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        Model.metadata.create_all(self.engine)

    def drop_tables(self):
        Model.metadata.drop_all(self.engine)

    def insert(self, model, rows: Union[Dict, List[Dict]]):
        """insert single or multiple rows into the table represented by the model

        Args:
            model: Model class representing the table
            rows: Single or multiple rows to be inserted

        """
        if not hasattr(model, "__table__"):
            raise TypeError("model must be a SQLAlchemy declarative model")

        if not rows:
            return

        with self.Session.begin() as session:
            session.execute(insert(model), rows)

    def execute_query(self, query):
        with self.engine.connect() as connection:
            connection.execute(query)
