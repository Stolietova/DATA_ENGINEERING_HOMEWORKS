# python_scripts/iris_ml_processor.py

from airflow.exceptions import AirflowFailException
import joblib 
import pandas as pd
import os
from sqlalchemy import create_engine, text


# test data
IRIS_CLASSES = {0: 'setosa', 1: 'versicolor', 2: 'virginica'}
MODEL_PATH_BASE = "/tmp/models/"

def run_inference(**kwargs):
    """
    Process Iris dataset from PostgreSQL, load a model, score new samples and save results.
    """
    # Get connection parameters from environment variables or use defaults
    pg_host = os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('ANALYTICS_DB', 'analytics')
    pg_user = os.getenv('ETL_USER', 'etl_user')
    pg_password = os.getenv('ETL_PASSWORD', 'etl_password')
    
    # Create SQLAlchemy engine for DataFrame operations
    conn_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(conn_string)

    ti = kwargs['ti']
    execution_date_str = kwargs.get('data_date') 
    
    model_path = ti.xcom_pull(task_ids='train_model', key='model_path')
    
    if not model_path or not os.path.exists(model_path):
        raise AirflowFailException(f"Інференс фейл: Файл моделі не знайдено за шляхом {model_path}.")
        
    try:
        # Load saved model
        loaded_model = joblib.load(model_path)
        print(f"Model successfully loaded from: {model_path}")
        
        # Query the processed Iris data from the dbt-transformed table
        inference_query = f"""
            SELECT * FROM homework.iris_processed 
            WHERE data_date = '{execution_date_str}'; 
        """

        new_samples = pd.read_sql(inference_query, engine)

        if new_samples.shape[0] == 0:
            raise AirflowFailException(
                f"TECHNICAL FAILURE: Missing input data (0 rows loaded for {execution_date_str}). "
                "Job must fail as processing cannot start."
            )
        
        # Top 5 features for score
        feature_columns = ti.xcom_pull(task_ids='train_model', key='top_5_features')

        X_score = new_samples[feature_columns]

        # Get prediction for new samples
        predictions_numeric = loaded_model.predict(X_score)
        
        new_samples['prediction_label'] = [IRIS_CLASSES[p] for p in predictions_numeric]
        
        results_df = new_samples[['id', 'data_date', 'prediction_label']]
        results_df['run_timestamp'] = pd.Timestamp.now()

        # Save results to PostgreSQL
        with engine.connect() as connection:
                # Create tables if they don't exist
            connection.execute("""
                               
            CREATE TABLE IF NOT EXISTS homework.iris_predictions (
                row_id SERIAL,
                id INTEGER NOT NULL,
                data_date DATE NOT NULL,
                prediction_label VARCHAR(100),
                run_timestamp TIMESTAMP,
                PRIMARY KEY (id, data_date)
            );
            """)

            # Idempotency
            delete_metrics_query = f"""
                DELETE FROM homework.iris_predictions
                WHERE data_date = '{execution_date_str}';
                """
            connection.execute(text(delete_metrics_query))

            # Save new samples score
            results_df.to_sql('iris_predictions', connection, schema='homework', 
                            if_exists='append', index=False)
            
        num_scored_records = len(results_df)

        # Return the length of scored records for XCom
        return {
            'len_scored_records': num_scored_records
        }
        
    except Exception as e:
       raise AirflowFailException(f"Inference runtime failure. Reason: {str(e)}")