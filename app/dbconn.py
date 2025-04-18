from sqlalchemy import text, create_engine
from .swift_code_parser import SwiftCodeParser
import os
from dotenv import load_dotenv
from pathlib import Path

class DBConn:
    def __init__(self):
        #todo: implement it using env files
        user = os.getenv('POSTGRES_USER', 'swift_api_db')
        dbname = os.getenv('POSTGRES_DB', 'swift_api_db')
        password = os.getenv('POSTGRES_PASSWORD', '?very_strong_password123?')
        
        connection_string = f"postgresql+psycopg://{user}:{password}@db:5432/{dbname}"
        self.__engine = create_engine(connection_string)
        
        #hardcoding it is much faster than querying the database
        global branch_column_names 
        branch_column_names = ["address", "bankName", "countryISO2", "countryName", "isHeadquarter", "swiftCode"]
        #branch_column_names = connection.execute(query).keys()
        #for k in branch_column_names:
        #    new_keys = []
        #    for key in branch_column_names:
        #        match key:
        #            case "ADDRESS":
        #                new_keys.append("address")
        #            case "NAME":
        #                new_keys.append("bankName")
        #            case "COUNTRY ISO2 CODE":
        #                new_keys.append("countryISO2")
        #            case _:
        #                new_keys.append(key)
        #    branch_column_names = new_keys
    
    def get_connection(self):
        """
        Returns a new raw connection.
        Remember to call connection.close() when done.
        """
        return self.__engine.connect()
    
    def populate_db(self):
        df = SwiftCodeParser().get_df() 
        df.to_sql('swift_codes', con=self.__engine, if_exists='replace', index=False)
    
    def get_swift_code(self, swift_code):
        with self.get_connection() as connection:
            print(f"Received SWIFT code: {swift_code}")
            query = text('''
                SELECT "ADDRESS", "NAME", "COUNTRY ISO2 CODE", "COUNTRY NAME", "ISHQ", "SWIFT CODE", "HQ SWIFT CODE"
                FROM swift_codes WHERE "SWIFT CODE" = UPPER(:swiftcode)
                ''')
            result = connection.execute(query, {'swiftcode':swift_code})
            
            rows = [dict(zip(branch_column_names, row)) for row in result]
            if rows:
                return rows[0]
            else:
                return None 
            
    