import pandas as pd
import numpy as np
import json
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from python.get import retrieve_data
from python.tools import get_from_db
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)

def analyzing_data():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        sql_file_path = "/opt/airflow/dags/sql/ANALYSIS_DATA.sql"
        conn = postgres_hook.get_conn()

        _, query_data = get_from_db(sql_file_path=sql_file_path, conn=conn)
        return query_data
    except Exception as e:
        print(f"Error - {e}")

def plot_model():
    try:
        raw_data = retrieve_data()
        data_analysis = analyzing_data()
        data = json.loads(raw_data)
        df = pd.DataFrame(data, columns=['id', 'date', 'ticker', 'result', 'profit'])
        data_analysis_df = pd.DataFrame(data_analysis, columns=['trade_name', 'profit', 'loss', 'profit_loss', 'change_profit_loss', 'win_trades', 'loss_trades', 'total_trades'])
        df['profitability'] = np.where(df['result'] == 'Win', 0, 1)

        logging.info("Sending report via email...") 
        # Different tables
        data_analysis_df.to_csv('/opt/airflow/dags/csv/Analysis.csv', index=False)
        df.groupby('ticker').describe()['profit'].to_csv('/opt/airflow/dags/csv/Report.csv')

        # Plot pair plot
        pair_plot = sns.pairplot(data=df, hue='ticker')
        pair_plot.fig.set_size_inches(28, 14)
        pair_plot.savefig('/opt/airflow/dags/images/PairPlot.png')
        plt.close()

        # Plot line chart
        trading_id = df['id']
        trading_result = (df['profit'].cumsum()).round(2)
        trading_data = pd.DataFrame({'id': trading_id, 'profit': trading_result})   
        fig, history_plot = plt.subplots(figsize=(34, 12))
        sns.lineplot(data=trading_data, x='id', y='profit', alpha=0.7, marker='o', label='Trades', ax=history_plot)
        coeff = np.polyfit(trading_id, trading_result, 1)
        trendline = np.poly1d(coeff)
        history_plot.plot(trading_id, trendline(trading_id), label="Trendline", color='red')
        for i, result in enumerate(trading_result):
            history_plot.text(trading_id[i], result, str(result), fontsize=7, fontweight='light', ha='left', va='bottom')
        history_plot.set_xlim(-1, 60)
        history_plot.set_ylim(-50, 300)
        history_plot.tick_params(axis='x', labelsize=7)
        history_plot.tick_params(axis='y', labelsize=7)
        history_plot.legend()
        fig.savefig('/opt/airflow/dags/images/HistoryProgress.png')
        plt.close()

        # Plot heat map
        heat_map = df.groupby(by=['ticker', 'result']).count()['profitability'].unstack()
        fig, heatmap_win_loss_count = plt.subplots(figsize=(28, 14))
        sns.heatmap(heat_map, annot=True, fmt=".2f", cmap="magma", ax=heatmap_win_loss_count)
        fig.savefig('/opt/airflow/dags/images/HeatMap.png')
        plt.close()
    except Exception as e:
        print(f"Error - {e}")