import pandas as pd
from app.quality.validation import validate_positive_sessions


def test_positive_sessions():

    df = pd.DataFrame({
        "session_duration": [10, 20, 30]
    })

    assert validate_positive_sessions(df)