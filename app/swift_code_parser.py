import pandas as pd

class SwiftCodeParser:
    """
    A class to parse and format SWIFT codes from a CSV file.
    """
    def __init__(self, FILE_PATH='./data/Interns_2025_SWIFT_CODES.csv'):
        self.file_path = FILE_PATH
        self._df = self._load_and_process()
        
        
    def _load_and_process(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path)
        df = self._identify_headquarters(df)
        self._format_codes(df)
        return df
        
    def _identify_headquarters(self, df: pd.DataFrame) -> pd.DataFrame:
        df['ISHQ'] = df['SWIFT CODE'].str.endswith('XXX').astype(int)
        headquarters = df[df['ISHQ'] == 1]['SWIFT CODE'].str[:8]
        
        hq_set = set(headquarters)
        df['HQ SWIFT CODE'] = df['SWIFT CODE'].str[:8].apply(
            lambda x: x + 'XXX' if x in hq_set else None
        )    
        return df

    def _format_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.loc[:, df.nunique() > 1]
        
        df.loc[:, 'COUNTRY ISO2 CODE'].str.upper()
        df.loc[:, 'COUNTRY NAME'].str.upper()
        
        columns_to_keep = [
            'COUNTRY ISO2 CODE',
            'SWIFT CODE', 
            'NAME', 
            'ADDRESS', 
            'HQ SWIFT CODE', 
            'ISHQ', 
            'TIME ZONE'
        ]

        df = df[columns_to_keep]
    
    async def get_df(self) -> pd.DataFrame:
        """
        Asynchronously retrieves the DataFrame associated with the instance.
        Returns:
            pd.DataFrame: The DataFrame stored in the instance.
        """
        return self._df