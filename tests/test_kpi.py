import pandas as pd
from app.analytics.kpi_calculations import calculate_avg_session


def test_avg_session():

    df = pd.DataFrame({
        "session_duration": [10, 20, 30]
    })

    assert calculate_avg_session(df) == 20