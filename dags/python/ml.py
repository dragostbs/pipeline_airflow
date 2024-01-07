import tensorflow as tf
import pandas as pd
import numpy as np
import json
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import  MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from python.get import retrieve_data

logging.basicConfig(level=logging.INFO)

def ml_model():
    try: 
        raw_data = retrieve_data()
        data = json.loads(raw_data)
        df = pd.DataFrame(data, columns=['id', 'date', 'ticker', 'result', 'profit'])
        df['profitability'] = np.where(df['result'] == 'Win', 0, 1)

        # Select Profit as the main feature to predict it
        X = df.drop(["profit", "date", "ticker", "result"], axis=1).values
        y = df["profit"].values

        # Split into train and test data at 30%
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
        
        # Standardize data on train in order to prevent leakege on test data
        scaler = MinMaxScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

        logging.info("AI Model Prediction...") 
        # Build the model
        nn_model = tf.keras.Sequential([
            tf.keras.layers.Dense(32, activation="relu"),
            tf.keras.layers.Dense(16, activation="relu"),
            tf.keras.layers.Dense(8, activation="relu"),
            tf.keras.layers.Dense(1)
        ])

        # Compile and fit the model
        nn_model.compile(optimizer=tf.keras.optimizers.Adam(0.001), loss="mse")
        nn_model.fit(x=X_train, y=y_train, validation_data=(X_test, y_test), batch_size=16, epochs=100)

        logging.info("Sending report via email...") 
        # Loss function plot
        loss_df = pd.DataFrame(nn_model.history.history)
        loss_plot = sns.lineplot(data=loss_df)
        fig = loss_plot.figure
        fig.set_size_inches(42, 18)
        fig.savefig('/opt/airflow/dags/images/MLModel.png')
        plt.close()

        # Predict the Profit using the model
        predictions = nn_model.predict(X_test)
        predictions_df = pd.DataFrame(predictions, columns=["Predicted_Profit"])
        predictions_df.to_csv('/opt/airflow/dags/csv/Predictions.csv', index=False)

        # Peformance stats of the model
        mse = mean_squared_error(y_test, predictions)
        mae = mean_absolute_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)

        metrics = pd.DataFrame({
            'MSE': [mse],
            'MAE': [mae],
            'R2': [r2]
        })

        metrics.to_csv('/opt/airflow/dags/csv/Metrics.csv', index=False)
    except Exception as e:
        print(f"Error - {e}")
        
