import psycopg2
import boto3
import os

dbname = "postgres"
user = "markov_chain_app"
password = os.environ.get("DB_PASSWORD")
host = "192.168.1.113"
port = "5432"

bucket_name = "markov-chain-generations"

def upload_to_s3(folder, file_name, data):
    try:
        full_path = folder + file_name
        print(full_path)
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=full_path, Body=data.encode('utf-8'))

    except Exception as e:
        print("Error:", e)

def get_initial_seed_word(category):
    word = ""
    connection = ""
    try:
        connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

        cursor = connection.cursor()

        query = f"""
            select markov_key
            from markov_key
            where category = '{category}'
            order by RANDOM()
            limit 1
            ;
        """

        cursor.execute(query)
        word = cursor.fetchone()[0]

        cursor.close()
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
    finally:
        # Close the connection
        if connection:
            connection.close()

        return word

def get_second_word_from_db(category, initial_key):
    word = ""
    try:
        connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

        cursor = connection.cursor()

        query = f"""
            select mv.markov_value
            from markov_key mk
            join markov_value mv
                on mk.id = mv.markov_key_id
            where mk.category = '{category}'
                and mk.markov_key = '{initial_key}'
            order by RANDOM()
            limit 1
            ;
        """

        cursor.execute(query)
        word = cursor.fetchone()[0]

        cursor.close()
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
    finally:
        # Close the connection
        if connection:
            connection.close()

        return word

def count_objects_in_bucket():
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket_name)
    object_count = objects.get('KeyCount', 0) -1

    return object_count

def main():
    max_retries = 10
    for run_num in range(count_objects_in_bucket(), 2000):
        print(run_num)
        retry_count = 0
        while retry_count < max_retries:
            poem = get_initial_seed_word("POEM")
            print(poem)
            for iteration in range(150):
                last_poem_word = poem.split()[-1]
                word = get_second_word_from_db("POEM", last_poem_word)

                if word == "" or word == None:
                    break

                poem = poem + " " + word

            if len(poem.split(' ')) > 100:
                print(run_num)
                file_name = f"markov_gen_poem_{run_num}_v1.txt"
                upload_to_s3("poems/", file_name, poem)
                break
            else:
                print("retrying")
                print(len(poem.split()))
                retry_count += 1

        if retry_count == max_retries:
            print(f"Max retries reached for iteration {run_num}")

if __name__ == '__main__':
    main()
