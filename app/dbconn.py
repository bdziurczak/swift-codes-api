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
        
        with self.get_connection() as connection:
            # Create a new table for country data
            connection.execute(text('''
                CREATE TABLE IF NOT EXISTS country_data (
                    "COUNTRY ISO2 CODE" VARCHAR PRIMARY KEY,
                    "COUNTRY NAME" VARCHAR NOT NULL
                )
            '''))
            connection.commit()
            
            # Populate the country_data table with distinct country information
            connection.execute(text('''
                INSERT INTO country_data ("COUNTRY ISO2 CODE", "COUNTRY NAME")
                SELECT DISTINCT "COUNTRY ISO2 CODE", "COUNTRY NAME"
                FROM swift_codes
            '''))
            connection.commit()
            
            # Remove the COUNTRY NAME column from the swift_codes table
            connection.execute(text('''
                ALTER TABLE swift_codes
                DROP COLUMN "COUNTRY NAME"
            '''))
            connection.commit()
    
    async def get_data_by_swift_code(self, swift_code: str) -> list[dict]:
        #hardcoding it is much faster than querying the database
        branch_column_names = ["address", "bankName", "countryISO2", "countryName", "isHeadquarter", "swiftCode"]
        async with self.get_aconnection() as connection:
            query = text('''
                SELECT sc."ADDRESS", sc."NAME", sc."COUNTRY ISO2 CODE", cd."COUNTRY NAME", sc."ISHQ", sc."SWIFT CODE", sc."HQ SWIFT CODE"
                FROM swift_codes sc
                JOIN country_data cd ON sc."COUNTRY ISO2 CODE" = cd."COUNTRY ISO2 CODE"
                WHERE sc."SWIFT CODE" = UPPER(:swiftcode)
                ''')
            result = await connection.execute(query, {'swiftcode':swift_code})
            
            rows = [dict(zip(branch_column_names, row)) for row in result]
                
            try:
                if not rows:
                    raise ValueError("No data found for the given SWIFT code.")
                # Differentiate between headquarter and branch
                is_headquarter = rows[0].get("isHeadquarter")
                if is_headquarter:
                    subquery = text('''
                        SELECT sc."ADDRESS", sc."NAME", sc."COUNTRY ISO2 CODE", cd."COUNTRY NAME", sc."ISHQ", sc."SWIFT CODE"
                        FROM swift_codes sc
                        JOIN country_data cd ON sc."COUNTRY ISO2 CODE" = cd."COUNTRY ISO2 CODE"
                        WHERE sc."HQ SWIFT CODE" = :hq_swift_code AND sc."SWIFT CODE" != :hq_swift_code
                    ''')
                    branches = await connection.execute(subquery, {'hq_swift_code': swift_code})
                    
                    branches_list = [dict(zip(branch_column_names, branch)) for branch in branches]
                    rows[0]["branches"] = branches_list
                return rows[0]
            except ValueError as e:
                return {"error": str(e)}
            except Exception as e:
                return {"error": "An unexpected error occurred.", "details": str(e)}
            
    async def get_data_by_country(self, countryISO2code: str) -> list[dict]:
        branch_column_names = ["address", "bankName", "countryISO2", "countryName", "isHeadquarter", "swiftCode"]
        async with self.get_aconnection() as connection:
            query = text('''
                SELECT cd."COUNTRY NAME"
                FROM country_data cd
                WHERE cd."COUNTRY ISO2 CODE" = UPPER(:countryISO2code)
            ''')
            query1 = text('''
                SELECT sc."ADDRESS", sc."NAME", sc."COUNTRY ISO2 CODE", sc."ISHQ", sc."SWIFT CODE"
                FROM swift_codes sc
                JOIN country_data cd ON sc."COUNTRY ISO2 CODE" = cd."COUNTRY ISO2 CODE"
                WHERE cd."COUNTRY ISO2 CODE" = UPPER(:countryISO2code)
            ''')
            country_name = await connection.execute(query, {'countryISO2code':countryISO2code})
            swift_codes = await connection.execute(query1, {'countryISO2code':countryISO2code})
            try:
                if not country_name or not swift_codes:
                    raise ValueError("No data found for the given country code.")
                
                rows = {'countryISO2': countryISO2code, 'countryName': country_name.scalar() if country_name else None}
                rows["swiftCodes"] = [dict(zip(branch_column_names, row)) for row in swift_codes]
                
                return rows
            except ValueError as e:
                return {"error": str(e)}
            except Exception as e:
                return {"error": "An unexpected error occurred.", "details": str(e)}    
    async def add_swift_code_entry(self, request: dict, required_fields: set[str]) -> dict:
        async with self.get_aconnection() as connection:
            try:
            #Insert the new SWIFT code entry into the swift_codes table
                query = text('''
                    INSERT INTO swift_codes ("ADDRESS", "NAME", "COUNTRY ISO2 CODE", "ISHQ", "SWIFT CODE")
                    VALUES (:address, :bankName, :countryISO2, :isHeadquarter, :swiftCode)
                ''')
                await connection.execute(query, {field: request[field] for field in required_fields})
                await connection.commit()

                # Check if the country already exists in the country_data table
                country_query = text('''
                    SELECT 1 FROM country_data
                    WHERE "COUNTRY ISO2 CODE" = :countryISO2
                ''')
                result = await connection.execute(country_query, {'countryISO2': request["countryISO2"]})
                if not result.scalar():
                    # Insert the country data if it doesn't exist
                    country_insert = text('''
                    INSERT INTO country_data ("COUNTRY ISO2 CODE", "COUNTRY NAME")
                    VALUES (:countryISO2, :countryName)
                    ''')
                    await connection.execute(country_insert, {
                    'countryISO2': request["countryISO2"],
                    'countryName': request["countryName"]
                    })
                    await connection.commit()

                return {"message": "SWIFT code entry added successfully."}
            except Exception as e:
                return {"error": "Failed to add SWIFT code entry.", "details": str(e)}
    async def delete_swift_code_entry(self, swift_code: str):    
        try:
            # Delete the SWIFT code entry from the swift_codes table
            query = text('''
            DELETE FROM swift_codes
            WHERE "SWIFT CODE" = UPPER(:swiftCode)
            ''')
            async with self.get_aconnection() as connection:
                result = await connection.execute(query, {'swiftCode': swift_code})
                await connection.commit()

                if result.rowcount == 0:
                    raise ValueError("No SWIFT code entry found for the given SWIFT code.")

                return {"message": "SWIFT code entry deleted successfully."}
        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": "Failed to delete SWIFT code entry.", "details": str(e)}