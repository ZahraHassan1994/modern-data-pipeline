def validate_nulls(df):
    return df.isnull().sum().sum() == 0


def validate_positive_sessions(df):
    return (df["session_duration"] > 0).all()