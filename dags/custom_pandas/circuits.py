import pandas as pd

def extract_circuits(circuits_file_path, **kwargs):
    df_circuits = pd.read_csv(circuits_file_path)
    # Push the data to XCom
    kwargs['ti'].xcom_push(key='circuits_data', value=df_circuits.to_dict())