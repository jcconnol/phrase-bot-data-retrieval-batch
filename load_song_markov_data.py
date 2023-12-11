
import os
import inflect
import re
import psycopg2
from psycopg2 import pool
import concurrent.futures
from langdetect import detect

num_threads = 5

db_params = {
    'dbname': "postgres",
    'user': "markov_chain_app",
    'password': os.environ.get("DB_PASSWORD"),
    'host': "johncc.local",
    'port': "5432"
}

connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=num_threads,
    **db_params
)

def numbers_to_words(input_string):
    p = inflect.engine()
    words = input_string.split()
    words = [p.number_to_words(word) if word.isdigit() else word for word in words]
    result = ' '.join(words)

    return result

def tokenize_strings(full_string):
    full_string = full_string.split("_____")[0]

    #remove commas and ()
    full_string = re.sub(r'[\(\),]', '', full_string)

    # replace .. or ... with .
    full_string = re.sub(r'\.{2,}', '.', full_string)

    full_string = numbers_to_words(full_string)

    full_string = re.sub(r'[\!?]', '.', full_string)

    full_string = re.sub(r'[^a-zA-Z0-9. ]', '', full_string)

    return \
        full_string \
            .upper() \
            .split()

def clean_tokens(tokenized_files):
    return tokenized_files

def insert_data(values):
    connection = None
    try:
        query = f"""
            WITH inserted_key AS (
                INSERT INTO public.markov_key (category, word_source, markov_key)
                VALUES ('SONG', %s, %s)
                ON CONFLICT (category,markov_key) DO UPDATE SET category = EXCLUDED.category
                RETURNING id
            )
            INSERT INTO public.markov_value (markov_key_id, markov_value)
            SELECT id, %s
            FROM inserted_key;
        """

        connection = connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()

    except psycopg2.Error as e:
        print("Error:", e)

    finally:
        if connection:
            connection_pool.putconn(connection)

def process_chunk(chunk):
    for values in chunk:
        insert_data(values)

def load_tokens(file_path, token_array):
    threads = []

    tuples_pairs = [(file_path, token_array[i], token_array[i + 1]) for i in range(len(token_array) - 1)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, tuples_pairs)]

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)


def read_and_load_files_data(directory_path):
    all_text = ""

    for root, dirs, files in os.walk(directory_path):
        # ending = 10
        # current = 0
        # stopping = False
        for file in files:
            file_path = os.path.join(root, file)

            with open(file_path, 'r', encoding='utf-8') as f:
                print(file)
                file_content = f.read()

                if detect(file_content) != "en":
                    continue

                tokenized_content = tokenize_strings(file_content)
                clean_content = clean_tokens(tokenized_content)

                load_tokens(file, clean_content)

                # if current == ending:
                #     stopping = True
                #     break
                # else:
                #     current += 1

        # if stopping:
        #     break

def handler(event, context):
    directory_path = "C:\\Users\\Jon\\Desktop\\software_eng\\phrase-bot-data-retrieval-batch\\songs\\"
    read_and_load_files_data(directory_path)
    connection_pool.closeall()

if __name__ == "__main__":
    handler({},{})
