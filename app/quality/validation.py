import pandas as pd

class DataValidator:

    @staticmethod
    def validate_schema(df: pd.DataFrame):
        required_cols = ["user_id", "country", "device", "session_duration", "timestamp"]
        return all(col in df.columns for col in required_cols)

    @staticmethod
    def validate_nulls(df: pd.DataFrame):
        return df.isnull().sum().sum() == 0

    @staticmethod
    def validate_session_duration(df: pd.DataFrame):
        return (df["session_duration"] > 0).all()