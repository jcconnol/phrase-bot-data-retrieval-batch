import psycopg2
import boto3

dbname = "postgres"
user = "markov_chain_app"
password = ""
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

def main():
    max_retries = 3
    for run_num in range(15):
        print(run_num)
        retry_count = 0
        while retry_count < max_retries:
            song = get_initial_seed_word("SONG")
            print(song)
            for iteration in range(150):
                last_song_word = song.split()[-1]
                word = get_second_word_from_db("SONG", last_song_word)

                if word == "" or word == None:
                    break

                song = song + " " + word

            if len(song.split(' ')) > 100:
                print(run_num)
                file_name = f"testing_{run_num}.txt"
                upload_to_s3("songs/", file_name, song)
                break
            else:
                print("retrying")
                print(len(song.split()))
                retry_count += 1

        if retry_count == max_retries:
            print(f"Max retries reached for iteration {_}")

if __name__ == '__main__':
    main()