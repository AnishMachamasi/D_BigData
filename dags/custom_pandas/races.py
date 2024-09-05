import pandas as pd

def extract_races(races_file_path, **kwargs):
    df_races = pd.read_csv(races_file_path)
    # Push the data to XCom
    kwargs['ti'].xcom_push(key='races_data', value=df_races.to_dict())