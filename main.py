import psycopg2
import os
import traceback
import logging
import pandas as pd
import numpy as np
import urllib.request
import subprocess
from dotenv import load_dotenv

load_dotenv()

def setup_logging():
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

def download_url(url: str, dest_folder: str, destination_path: str ):
    if not os.path.exists(str(dest_folder)):
        os.makedirs(str(dest_folder))
    try:
        if not os.path.exists(str(destination_path)):
            urrlib.request.urlretrieve(url, destination_path)
            logging.info('csv file downloaded sucessfully to the working directory')
        else: 
            logging.info('This file exists')
    except Exception as e:
        logging.error(f'Error while downloading the csv file due to: {e}')
        traceback.print_exc()

def connect_DB():
    try:
        connDB = psycopg2.connect(
            host= os.environ.get('postgres_host'),
            database = os.environ.get('postgres_database'),
            user = os.environ.get('postgres_user'),
            password = os.environ.get('postgres_password'),
            port= os.environ.get('postgres_port')
        )
        logging.info('Postgres server connection is successful')
    except Exception as e:
        traceback.print_exc()
        logging.error("Couldn't create the Postgres connection")

    return connDB


def create_DB():
    try:
        p =  subprocess.Popen(r"C:\Users\juan\Documents\project_data\createdb.bat", shell=True, stdout = subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            logging.info("The Database was created if not exists successfully")
        else:
            raise Exception()
    except Exception as error:
        traceback.print_exc()
        logging.error('The data base could not be created')

def create_schema(connDB):
    try:
        with connDB:
            with connDB.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS QA;")
                cur.execute("CREATE SCHEMA IF NOT EXISTS PROD;")
                cur.execute("CREATE SCHEMA IF NOT EXISTS DEV;")
            logging.info('schemas was created sucessfuly')

        connDB.commit()

    except Exception as error:
        traceback.print_exc()
        logging.error('The schemas could not be created')


def download_file(url, dest_folder, destination_path):
    try:
        download_url(url, dest_folder, destination_path)
    except Exception as error:
        traceback.print_exc()
        logging.error('The file could not be downladed')

def create_table(connDB, environment):
    try:
        with connDB:
            with connDB.cursor() as cur:
                    cur.execute(f"""CREATE TABLE {environment}.churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(100), Gender VARCHAR(40), Age INTEGER, Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""") #in the bbooleans == integer

                    cur.execute(f"""CREATE TABLE {environment}.churn_modelling_new_table (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(100), Gender VARCHAR(40), Age INTEGER, Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")
        connDB.commit()

        logging.info(f'New table {environment}.churn_modelling created sucessfully to postgres server')

    except:
        logging.warning(f'The table {environment}.churn_modelling exists')


def write_to_postgres(connDB, dest_folder, environment):
    df = pd.read_csv(f'{dest_folder}/churn_modelling.csv')
    inserted_row_count = 0
    with connDB:
        with connDB.cursor() as cur:
            for _, row in df.iterrows():
                count_query = f"""SELECT COUNT(*) FROM {environment}.churn_modelling WHERE RowNumber={row['RowNumber']};"""
                count_query_new =  f"""SELECT COUNT(*) FROM {environment}.churn_modelling_new_table WHERE RowNumber={row['RowNumber']};"""
                
                try: 
                    cur.execute(count_query)
                    result = cur.fetchone()

                except Exception as e :
                    print(f'Error {e}')
                    print('Anything else that you feel is useful')
                    connDB.rollback()
                if result[0] == 0:
                    inserted_row_count += 1 #insertmany
                    cur.execute(f"""INSERT INTO {environment}.churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))
                    cur.execute(f"""INSERT INTO {environment}.churn_modelling_new_table (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))
    
        connDB.commit()
    
    logging.info(f' {inserted_row_count} rows from csv file inserted into {environment}.churn_modelling table sucessfully')

def create_base_df(connDB, environment):
    with connDB: 
        with connDB.cursor() as cur:
            cur.execute(f"""SELECT * FROM {environment}.churn_modelling""")
            rows= cur.fetchall()

            col_names = [desc[0]for desc in cur.description]
            df = pd.DataFrame(rows, columns = col_names)

            df.drop('rownumber', axis=1, inplace=True)#pq esta borrando la tabla
            index_to_be_null = np.random.randint(1000, size=30) 
        
            df.loc[index_to_be_null, ['balance', 'creditscore', 'geography']] = np.nan

            most_occured_country = df['geography'].value_counts().index[0]
            df['geography'].fillna(value=most_occured_country, inplace=True) 

            avg_balance= df['balance'].mean()
            df['balance'].fillna(value=avg_balance, inplace=True)

            median_creditscore= df['creditscore'].median()
            df['creditscore'].fillna(value= median_creditscore, inplace=True)

    return df

def create_creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography', 'gender']).agg({'creditscore':'mean', 'exited':'sum'})

    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'},inplace=True)
    df_creditscore.reset_index(inplace = True)

    df_creditscore.sort_values('avg_credit_score', inplace=True)
    return df_creditscore

def create_exited_age_correlation(df): #crear correlacion de edad de salida?
    age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({'age':'mean', 'estimatedsalary': 'mean', 'exited':'count'}).rename(columns={
        'age':'avg_age',
        'estimatedsalary':'avg_salary',
        'exited':'number_of_exited_or_not'
    }).reset_index().sort_values('number_of_exited_or_not')

    return age_correlation

def create_exited_salary_correlation(df):
    salary_correlation = df[['geography', 'gender', 'exited', 'estimatedsalary']].groupby(['geography', 'gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
    salary_correlation.reset_index(inplace=True) 
    #ejem yo aqui agruparia por tenuere tambien

    min_salary = round(salary_correlation['estimatedsalary'].min(), 0)

    df['high_salary'] = df['estimatedsalary'].apply(lambda x:1 if x > min_salary else 0)

    df_exited_salary_correlation = pd.DataFrame({'exited': df['exited'], 
    'high_salary': 
df['estimatedsalary'] > df['estimatedsalary'].min(), 
    'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })
    #esta tomando los valores de el dataframe 
    
    return df_exited_salary_correlation

#siempre cuando se crea una tabla () se coloca como paramtero conn DB?
def create_new_table(connDB, environment):
    with connDB:
        with connDB.cursor() as cur:
            try:
                cur.execute(f"""CREATE TABLE IF NOT EXISTS {environment}.churn_modelling_creditscore(geography VARCHAR(50), gender VARCHAR(30), total_exited INTEGER, avg_credit_score FLOAT)""")
                cur.execute(f"""CREATE TABLE IF NOT EXISTS {environment}.churn_modelling_age(geography VARCHAR(50), gender VARCHAR(30), avg_age FLOAT, exited INTEGER, avg_salary FLOAT, number_of_exited_or_not INTEGER)
                """)
                cur.execute(f"""CREATE TABLE IF NOT EXISTS {environment}.churn_modelling_salary(exited INTEGER, high_salary INTEGER, correlation INTEGER) 
                """)
                #son 3 cur.execute x cada funcion
                logging.info("3 tables created sucessfully in Postgres server")
            except Exception as e:
                traceback.print_exc() #reforzar el .print_exc()
                logging.error(f"Tables cannot be created due to: {e}" ) 
                
def insert_creditscore_table(connDB, df_creditscore, environment):
    query =(f"""INSERT INTO {environment}.churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES(%s, %s, %s, %s)""")
    row_count = 0
    for _, row in df_creditscore.iterrows():
        with connDB:
            with connDB.cursor() as cur:
                values = (row['geography'], row['gender'], row['avg_credit_score'], row ['total_exited'])
                cur.execute(query, values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table {environment}.churn_modelling_creditscore")

def  insert_age_table(connDB, age_correlation, environment):
    query_age = (f"""INSERT INTO {environment}.churn_modelling_age (geography, gender, avg_age, exited, avg_salary, number_of_exited_or_not) VALUES(%s, %s, %s, %s, %s, %s)""")
    row_count=0
    for _, row in age_correlation.iterrows():
        with connDB:
            with connDB.cursor() as cur:
                values= (row['geography'], row['gender'], row['avg_age'], row['exited'], row['avg_salary'], row['number_of_exited_or_not'])
                cur.execute(query_age, values)
        row_count += 1
    logging.info(f"{row_count} rows inserted into table churn_modelling_age")

def insert_salary_table(connDB, df_exited_salary_correlation, environment):
    query_salary = (f"""INSERT INTO {environment}.churn_modelling_salary (exited, high_salary, correlation) VALUES (%s, %s, %s)""")
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        with connDB:
            with connDB.cursor() as cur:
                values=(int(row['exited']), int(row['high_salary']), int(row['correlation']))
                #int porque ?
                cur.execute(query_salary, values)
        row_count =+ 1
    logging.info(f"{row_count} rows inserted into table {environment}.churn_modelling_salary")

def main():
    url= "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv",
    dest_folder = os.environ.get('dest_folder')
    destination_path= f'{dest_folder}/churn_modelling.csv'
    setup_logging()
    connDB = connect_DB()
    create_DB()
    create_schema(connDB)
    download_file(url, destination_path, dest_folder)

    for i in ["dev", "qa", "prod"]:
        create_table(connDB, environment=i)
        create_new_table(connDB, environment=i)
        write_to_postgres(connDB, dest_folder, environment=i)
        df = create_base_df(connDB, environment=i)

    
    df_creditscore = create_creditscore_df(df)
    age_correlation = create_exited_age_correlation(df)
    df_exited_salary_correlation = create_exited_salary_correlation(df)

    for i in ["dev", "qa", "prod"]:
        insert_creditscore_table(connDB, df_creditscore, environment=i)
        insert_age_table(connDB, age_correlation, environment=i)
        insert_salary_table(connDB, df_exited_salary_correlation, environment=i)

    connDB.close()

if __name__ == "__main__": 
    main()