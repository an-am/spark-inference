import joblib
import pandas as pd
import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from tensorflow.keras.models import load_model
import subprocess

# Start the pg_listener.py script in the background
listener_proc = subprocess.Popen(["python", "pg_listener.py"])

NUM_MAX_REQUESTS = 5
HOST = 'localhost'
PORT = 9999

db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "port": "5432"
}

scaler_path = 'deep_scaler.pkl'
fitted_lambda_wealth, fitted_lambda_income = [0.1336735055366279, 0.3026418664067109]
column_names = [
    "Id", "Age", "Gender", "FamilyMembers", "FinancialEducation",
    "RiskPropensity", "Income", "Wealth", "IncomeInvestment",
    "AccumulationInvestment", "FinancialStatus", "ClientId"
]


def data_prep(data):
    # Compute FinancialStatus
    data['FinancialStatus'] = data['FinancialEducation'] * np.log(data['Wealth'])
    financial_status = data['FinancialStatus']
    print(f"financial_status: {financial_status}")

    # Drop target and unnecessary columns
    data = data.drop('Id', axis=1)
    data = data.drop('RiskPropensity', axis=1)
    data = data.drop('ClientId', axis=1)

    # Data prep
    data['Wealth'] = (data['Wealth'] ** fitted_lambda_wealth - 1) / fitted_lambda_wealth
    data['Income'] = (data['Income'] ** fitted_lambda_income - 1) / fitted_lambda_income

    scaler = joblib.load(scaler_path)
    data_scaled = scaler.transform(data)

    return data_scaled, financial_status.iloc[0]


def predict(data):
    model = load_model('deep_model.h5')
    return model.predict(data, verbose=0)[0][0]


def process_batch(df: pyspark.sql.dataframe.DataFrame, epoch_id):
    import psycopg2

    # Open a connection to PostgreSQL for this microbatch
    conn = psycopg2.connect(**db_config)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    df = df.collect()

    for row in df:
        row = pd.DataFrame([row.asDict()])
        if row.empty:
            return

        print(f"Processing row id {row['Id'].iloc[0]} for client {row['ClientId'].iloc[0]}...")

        # Data prep
        data_scaled, financial_status = data_prep(row)

        # Prediction
        prediction = predict(data_scaled)
        print(f"Predicted RiskPropensity for ID={row['Id'].iloc[0]}: {prediction}")


        # Update DB: financial_Status and risk_propensity
        query = f"""UPDATE needs 
                    SET risk_propensity  = {prediction},
                        financial_status = {financial_status}
                    WHERE id = {row['Id'].iloc[0]}"""
        cursor.execute(query)

        print(f"Updated for id = {row['Id'].iloc[0]} risk_propensity = {prediction} "
              f"and financial_status={financial_status}")

        # Get best products
        query = f"""SELECT * 
                    FROM products
                    WHERE   
                        (
                            Income  = {row['IncomeInvestment'].iloc[0]} 
                            OR Accumulation = {row['AccumulationInvestment'].iloc[0]}
                        )
                        AND Risk <= {prediction}"""
        cursor.execute(query)
        tuples_list = cursor.fetchall()

        # Print the results
        print(f"Advised {len(tuples_list)} products for id = {row['Id'].iloc[0]}")

    cursor.close()
    conn.close()


def update_request_count(client_id, pdf, state: GroupState):
    import psycopg2

    # Log the client_id for debugging purposes
    print(f"Processing client_id: {client_id}")

    # Initialize state if not available; state holds the running count for this client
    current_count = state.get[0] if state.exists else 0
    allowed_rows = []

    #pdf = pd.concat(list(pdf))

    for row in pdf:

        if current_count < NUM_MAX_REQUESTS:
            allowed_rows.append(row)
            current_count += 1

            # Update state with the new count (you can also add client_id here if desired)
            new_state = (current_count,)
            state.update(new_state)

            # Connect to Postgre
            conn = psycopg2.connect(**db_config)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()


            query = f"""SELECT * 
                        FROM needs 
                        WHERE id = {row['id'].iloc[0]}"""
            cursor.execute(query)

            a = cursor.fetchall()
            # print(f"tuple: {tuple}")
            df = pd.DataFrame(a, columns=column_names)

            cursor.close()
            conn.close()

            yield df

        else:
            # Optionally log that this client's quota is exceeded
            print(f"Client {client_id} reached max requests: {current_count}")
            yield pd.DataFrame(columns=column_names)


payload_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("client_id", IntegerType(), True)
])

schema = StructType(
    [StructField('Id', IntegerType(), False),
     StructField('Age', IntegerType(), True),
     StructField('Gender', IntegerType(), True),
     StructField('FamilyMembers', IntegerType(), True),
     StructField('FinancialEducation', FloatType(), True),
     StructField('RiskPropensity', FloatType(), True),
     StructField('Income', FloatType(), True),
     StructField('Wealth', FloatType(), True),
     StructField('IncomeInvestment', IntegerType(), True),
     StructField('AccumulationInvestment', IntegerType(), True),
     StructField('FinancialStatus', FloatType(), True),
     StructField('ClientId', FloatType(), False)])

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("SparkInference") \
        .master("local[10]") \
        .getOrCreate()

    # Set Spark logging level to ERROR.
    spark.sparkContext.setLogLevel("ERROR")

    socket_df = spark.readStream \
        .format("socket") \
        .option("host", HOST) \
        .option("port", PORT) \
        .load()

    # Check if DataFrame is streaming or Not.
    print(f"Streaming DataFrame: {socket_df.isStreaming}")

    parsed_df = socket_df \
        .select(from_json(col("value"), payload_schema).alias("data")) \
        .select("data.*")

    stateful_df = parsed_df \
        .groupBy("client_id") \
        .applyInPandasWithState(
        func=update_request_count,
        outputStructType=schema,
        stateStructType="count INT",
        outputMode="append",
        timeoutConf="ProcessingTimeTimeout")

    query = stateful_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

    listener_proc.terminate()
