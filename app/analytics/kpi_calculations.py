import pandas as pd


def calculate_avg_session(df):
    return df["session_duration"].mean()


def calculate_total_users(df):
    return df["user_id"].nunique()