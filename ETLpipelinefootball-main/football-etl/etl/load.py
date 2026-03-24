from sqlalchemy import create_engine

def load_data(df):
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    # engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')
    df.to_sql('top_scorers', engine, if_exists='replace', index=False)