import warnings
warnings.filterwarnings("ignore")

from typing import Iterator
import joblib
import pandas as pd
import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
import tensorflow as tf
import psycopg2


NUM_MAX_REQUESTS = 5
HOST = "localhost:9092"
TOPIC = "notifications"

db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "port": "5432"
}

scaler_path = '/Users/antonelloamore/PycharmProjects/spark-inference/deep_scaler.pkl'
model_path = '/Users/antonelloamore/PycharmProjects/spark-inference/deep_model.h5'

fitted_lambda_wealth, fitted_lambda_income = [0.1336735055366279, 0.3026418664067109]

column_names = [
    "Id", "Age", "Gender", "FamilyMembers", "FinancialEducation",
    "RiskPropensity", "Income", "Wealth", "IncomeInvestment",
    "AccumulationInvestment", "FinancialStatus", "ClientId"
]

feature_names = [
    "Age", "Gender", "FamilyMembers",
    "FinancialEducation", "Income", "Wealth",
    "IncomeInvestment", "AccumulationInvestment", "FinancialStatus"
]

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

prediction_schema = StructType(
    [StructField('Id', IntegerType(), False),
     StructField('RiskPropensity', FloatType(), False)])


_model = None
def load_model():
    global _model
    if _model is None:
        _model = tf.keras.models.load_model(model_path)
    return _model


_scaler = None
def load_scaler():
    global _scaler
    if _scaler is None:
        _scaler = joblib.load(scaler_path)
    return _scaler


_cursor = None
def load_cursor():
    global _cursor
    if _cursor is None:
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        _cursor = conn.cursor()
    return _cursor


def data_prep(data: DataFrame) -> DataFrame:
    return data \
        .withColumn("FinancialStatus", col("FinancialEducation") * log(col("Wealth"))) \
        .withColumn("Wealth", (pow(col("Wealth"), fitted_lambda_wealth) - 1) / fitted_lambda_wealth) \
        .withColumn("Income", (pow(col("Income"), fitted_lambda_income) - 1) / fitted_lambda_income) \
        .drop("ClientId")


'''
@pandas_udf(FloatType(), PandasUDFType.SCALAR_ITER)
def predict_udf(iterator: Iterator[
    Tuple[pd.Series, pd.Series, pd.Series,
    pd.Series, pd.Series, pd.Series,
    pd.Series, pd.Series, pd.Series]]
                ) -> Iterator[pd.Series]:
    for age, gender, family_members, fin_ed, income, wealth, inc_inv, acc_inv, fin_status in iterator:
        pd_data = pd.concat([age, gender, family_members, fin_ed, income, wealth, inc_inv, acc_inv, fin_status],
                            axis=1).to_numpy()

        scaler = load_scaler()
        pd_data_scaled = scaler.transform(pd_data)
        ##############
        # Assemble the inputs into a small DataFrame chunk
        data = pd.DataFrame({
            'Age': age,
            'Gender': gender,
            'FamilyMembers': family_members,
            'FinancialEducation': fin_ed,
            'Income': income,
            'Wealth': wealth,
            'IncomeInvestment': inc_inv,
            'AccumulationInvestment': acc_inv,
            'FinancialStatus': fin_status
        })

        scaler = load_scaler()
        data_scaled = scaler.transform(data)
        ##############
        model = load_model()

        predictions = model.predict(pd_data_scaled, verbose=0).flatten()
        yield pd.Series(list(predictions))
'''

def predict_partition(rows):
    # global model reuse pattern
    model = load_model()
    scaler = load_scaler()
    for row in rows:
        row_id = row["Id"]
        features = np.array([[row[f] for f in feature_names]], dtype=float)
        features_scaled = scaler.transform(features)
        pred = float(model.predict(features_scaled.reshape(1, -1), verbose=0)[0][0])
        # append prediction to row (could yield a dict or a new Row)
        yield (row_id, pred)

def query_db(row):
    # Open a connection to PostgreSQL for this microbatch
    cursor = load_cursor()

    print(f"row: {row}")

    # Update DB: financial_Status and risk_propensity
    query = f"""UPDATE needs 
                SET risk_propensity  = {row['RiskPropensity']},
                    financial_status = {row['FinancialStatus']}
                WHERE id = {row['Id']}"""
    cursor.execute(query)

    print(f"Updated for id = {row['Id']} risk_propensity = {row['RiskPropensity']} "
          f"and financial_status={row['FinancialStatus']}")

    # Get best products
    query = f"""SELECT * 
                FROM products
                WHERE   
                    (
                        Income  = {row['IncomeInvestment']} 
                        OR Accumulation = {row['AccumulationInvestment']}
                    )
                    AND Risk <= {row['RiskPropensity']}"""
    cursor.execute(query)
    tuples_list = cursor.fetchall()

    # Print the results
    print(f"Advised {len(tuples_list)} products for id = {row['Id']}")


def process_batch(df: pyspark.sql.dataframe.DataFrame, epoch_id):
    # Data prep
    df = df.transform(data_prep)

    # Prediction + queries
    result_rdd = df.drop('RiskPropensity').rdd.mapPartitions(predict_partition)

    result_df = spark.createDataFrame(result_rdd, schema=prediction_schema) \
        .join(df, on=['Id'], how='outer')

    result_df.rdd.foreach(query_db)


def update_request_count(client_id, pdf: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    # Log the client_id for debugging purposes
    #print(f"Processing client_id: {client_id}")

    # Initialize state if not available; state holds the running count for this client
    current_count = state.get[0] if state.exists else 0

    for client in pdf:
        for row in client.itertuples(index=False):
            id = row[0]
            client_id = row[1]

            #print(f"Processing client {client_id}, current_count: {current_count}")

            if current_count < NUM_MAX_REQUESTS:
                current_count += 1

                # Update state with the new count (you can also add client_id here if desired)
                new_state = (current_count,)
                state.update(new_state)

                # Connect to Postgres
                cursor = load_cursor()

                query = f"""SELECT * 
                            FROM needs 
                            WHERE id = {id}"""
                cursor.execute(query)

                result = cursor.fetchall()

                df = pd.DataFrame(result, columns=column_names)

                yield df

            #else:
                # Log that this client's quota is exceeded
                #print(f"Client {client_id} reached max requests: {current_count}")
                # yield pd.DataFrame(columns=column_names)


if __name__ == "__main__":
    import os

    os.environ[
        "PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.slf4j:slf4j-api:1.7.36 pyspark-shell"
    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("SparkInference") \
        .master("spark://master:7077") \
        .config("spark.jars.excludes", "org.slf4j:slf4j-api") \
        .getOrCreate()

    # Set Spark logging level to ERROR.
    spark.sparkContext.setLogLevel("ERROR")

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", HOST) \
        .option("subscribe", TOPIC) \
        .load()

    # Check if DataFrame is streaming or Not.
    print(f"Streaming DataFrame: {kafka_df.isStreaming}")

    # Convert the binary "value" field to string and parse JSON
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), payload_schema).alias("data")) \
        .select("data.*")

    stateful_df = parsed_df \
        .groupBy("client_id") \
        .applyInPandasWithState(
            func=update_request_count,
            outputStructType=schema,
            stateStructType="count INT",
            outputMode="append",
            timeoutConf="NoTimeout")
        #.filter("Id is not null")

    query = stateful_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
