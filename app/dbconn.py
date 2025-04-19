from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from .swift_code_parser import SwiftCodeParser
import os
from dotenv import load_dotenv
from pathlib import Path

class DBConn:
    def __init__(self):
        #todo: implement it using env files
        __user = os.getenv('POSTGRES_USER', 'swift_api_db')
        __dbname = os.getenv('POSTGRES_DB', 'swift_api_db')
        __password = os.getenv('POSTGRES_PASSWORD', '?very_strong_password123?')
        __port = os.getenv('POSTGRES_PORT', '5432')
        
        connection_string = f"postgresql+psycopg://{__user}:{__password}@db:{__port}/{__dbname}"
        self.__aengine = create_async_engine(connection_string, echo=True)
        self.__engine = create_engine(connection_string, echo=True)
        #hardcoding it is much faster than querying the database
        global branch_column_names 
        branch_column_names = ["address", "bankName", "countryISO2", "countryName", "isHeadquarter", "swiftCode"]
    
    def get_connection(self):
        """
        Returns a new raw connection.
        Remember to call connection.close() when done.
        """
        return self.__engine.connect()
    def get_aconnection(self):
        """
        Returns a new async connection.
        Remember to call connection.close() when done.
        """
        return self.__aengine.connect()
    
    def populate_db(self):
        df = SwiftCodeParser().get_df() 
        df.to_sql('swift_codes', con=self.__engine, if_exists='replace', index=False)
    
    async def get_data_by_swift_code(self, swift_code: str) -> list[dict]:
        async with self.get_aconnection() as connection:
            query = text('''
                SELECT "ADDRESS", "NAME", "COUNTRY ISO2 CODE", "COUNTRY NAME", "ISHQ", "SWIFT CODE", "HQ SWIFT CODE"
                FROM swift_codes WHERE "SWIFT CODE" = UPPER(:swiftcode)
                ''')
            result = await connection.execute(query, {'swiftcode':swift_code})
            
            rows = [dict(zip(branch_column_names, row)) for row in result]
                
            if rows:
                #differentiate between headquarter and branch
                is_headquarter = rows[0].get("isHeadquarter")
                if is_headquarter:
                    subquery = text('''
                        SELECT "ADDRESS", "NAME", "COUNTRY ISO2 CODE", "COUNTRY NAME", "ISHQ", "SWIFT CODE"
                        FROM swift_codes WHERE "HQ SWIFT CODE" = :hq_swift_code
                    ''')
                    branches = await connection.execute(subquery, {'hq_swift_code':swift_code})
                    
                    branches_list = [dict(zip(branch_column_names, branch)) for branch in branches]
                    rows[0]["branches"] = branches_list
                return rows[0]
            else:
                return "No data found for the given SWIFT code."
    async def get_data_by_country(self, countryISO2code: str) -> list[dict]:
        pass     
    