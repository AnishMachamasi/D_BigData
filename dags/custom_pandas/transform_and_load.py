import pandas as pd

def transform_data(**kwargs):
    # Pull the data from XCom
    circuits_data = kwargs['ti'].xcom_pull(key='circuits_data')
    races_data = kwargs['ti'].xcom_pull(key='races_data')

    df_circuits = pd.DataFrame(circuits_data)
    df_races = pd.DataFrame(races_data)

    # Example transformation: Merging circuits and races on circuitId
    df_merged = pd.merge(df_races, df_circuits, on='circuitId')

    # Additional transformations can be added here

    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=df_merged.to_dict())

def load_data(**kwargs):
    # Pull the transformed data from XCom
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data')
    df_transformed = pd.DataFrame(transformed_data)

    # Save the transformed data to a new CSV file or a database
    output_path = '/opt/airflow/dags/custom_pandas/f1_data_transformed.csv'
    df_transformed.to_csv(output_path, index=False)