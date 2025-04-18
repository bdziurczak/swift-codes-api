from sqlalchemy import create_engine, text
from .swift_code_parser import SwiftCodeParser

class DBConn:
    def __init__(self):
        self.FILE_PATH = './app/data/Interns_2025_SWIFT_CODES.csv'
        user = 'swift_api_db'
        dbname = 'swift_api_db'
        password = '?very_strong_password123?'
        connection_string = f"postgresql+psycopg://{user}:{password}@db:5432/{dbname}"
        self.engine = create_engine(connection_string)
    
    def get_connection(self):
        """
        Returns a new raw connection.
        Remember to call connection.close() when done.
        """
        return self.engine.connect()
    def populate_db(self):
        df = SwiftCodeParser(self.FILE_PATH).get_df() 
        df.to_sql('swift_codes', con=self.engine, if_exists='replace', index=False)
        