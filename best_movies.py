import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime, time
import requests
import pandas as pd

# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns


def log_progress(message, log_file):
    with open(log_file, 'a') as f:
        time_now = datetime.now()
        date_formate = '%Y-%m-%d %A    %H:%M:%S'
        date = time_now.strftime(date_formate)
        f.write(message+'   '+date+'\n')
        return True


def request_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("Request Successeded!")
    else:
        print(f"Failed with status code: {response.status_code}")

    return response


def scrap_movies_info(response):
    html_hir = BeautifulSoup(response.content, 'html.parser')
    li_movies = html_hir.find_all(
        'li', class_='ipc-metadata-list-summary-item sc-10233bc-0 TwzGn cli-parent')
    movies = [x.find('div').find_next_sibling() for x in li_movies]
    movies_info = [x.find('span').find_next_sibling() for x in movies]

    df = pd.DataFrame(
        columns=['Title', 'Release_Year', 'Length', 'Kind', 'Rate'])

    for m in movies_info:
        title = m.find('h3').text
        info = m.find('div').find_next_sibling().find_all('span')
        releae_year = info[0].text
        length = info[1].text
        kind = info[2].text
        rate = m.find('div').find_next_sibling(
            'span').find('svg').find_next_sibling().text

        record = {
            'Title': title,
            'Release_Year': int(releae_year),
            'Length': length,
            'Kind': kind,
            'Rate': float(rate)
        }

        record = pd.DataFrame(record, index=[0])
        df = pd.concat([df, record], ignore_index=True)

    return df


def transform_data(df):
    df['Title'] = df['Title'].apply(lambda x: x[x.find(' '):].strip())

    length = df['Length'].str.split()
    new_length = []
    for l in length:
        h = l[0][:-1]
        m = l[1][:-1]
        lon = time(hour=int(h), minute=int(m))
        time_formate = '%H:%M'
        l = time.strftime(lon, time_formate)
        new_length.append(l)
    new_length = pd.Series(new_length)
    df['Length'] = new_length

    rating = df['Kind']
    new_rate = []
    for r in rating:
        if r == '13+':
            r = 'PG-13'
            new_rate.append(r)
        elif r == '16+':
            r = 'R'
            new_rate.append(r)
        elif r == '18+':
            r = 'NC-17'
            new_rate.append(r)
        else:
            new_rate.append(r)
    new_rate = pd.Series(new_rate)
    df['Kind'] = new_rate
    return df


def load_db(df, table, sql_db):
    try:
        conn = sqlite3.connect(sql_db)
        df.to_sql(name=table, con=conn, if_exists='replace', index=True)
        conn.commit()
        conn.close()
        return 'Data Saved in DB Successfuly'
    except Exception as e:
        return (f"Error saving DB: {e}")


def load_json(df, to_json):
    try:
        df.to_json(to_json, orient='index')
        return 'Data Saved in JSON Successfuly'
    except Exception as e:
        return (f"Error saving JSON: {e}")


def load_csv(df, to_csv):
    try:
        df.to_csv(to_csv)
        return 'Data Saved in CSV Successfuly'
    except Exception as e:
        return (f"Error saving CSV: {e}")


def execute_sql_query(query_statement, param, sql_db):
    try:
        conn = sqlite3.connect(sql_db)
        result = pd.read_sql(query_statement, params=param, con=conn)
        conn.commit()
        conn.close()
    except Exception as e:
        return (f"Error processing query: {e}")


if __name__ == '__main__':
    url = 'https://www.imdb.com/chart/top'
    to_json = 'Best_movies.json'
    to_csv = 'Best_movies.csv'
    sql_table = 'Best_movies'
    sql_db = 'Best_movies.db'
    log_file = 'log.txt'

    log_progress('ETL Process Started', log_file)

    response = request_page(url)
    log_progress('Request URL Started', log_file)
    if response.status_code == 200:
        log_progress('Request Responded Successfuly', log_file)

        df = scrap_movies_info(response=response)

        log_progress('Data Extraction Started', log_file)

        if not df.empty:
            log_progress('Data Extracted Successfuly', log_file)
        else:
            log_progress('Data Extracted failed', log_file)

        log_progress('Data Transformation Started', log_file)

        df_trns = transform_data(df)

        if not df_trns.empty:
            log_progress('Data Transformed Successfuly', log_file)
        else:
            log_progress('Data Transformed failed', log_file)

        df = df_trns

        message = load_db(df, sql_table, sql_db)
        log_progress(message, log_file)

        message = load_json(df, to_json)
        log_progress(message, log_file)

        message = load_csv(df, to_csv)
        log_progress(message, log_file)

        log_progress('ETL Process Ended', log_file)

        query = (f'SELECT * FROM {sql_table}')
        param = []
        result = execute_sql_query(query, param, sql_db)

        print('\n\nHere are the result of Select All query from DB\n')
        print(result)

        # Optionally, reset display options to default values
        pd.reset_option('display.max_rows')
        pd.reset_option('display.max_columns')
    else:
        log_progress(
            f'Sending Request Failed With Code: {response.status_code}', log_file)
        log_progress('ETL Process Ended', log_file)
