import requests
import time
import sqlite3
import schedule


class CoinGeckoAPI:
    def __init__(self):
        # Initialize base URL for the CoinGecko API
        self.base_url = "https://api.coingecko.com/api/v3/"

    def fetch_token_data(self, vs_currency="usd", per_page=250, page=1):
        # Fetches token data from CoinGecko, handling errors and rate limits
        endpoint = "coins/markets"
        params = {
            "vs_currency": vs_currency,
            "per_page": per_page,
            "page": page
        }

        try:
            response = requests.get(self.base_url + endpoint, params=params)

            if response.status_code == 200:
                # Successful response, return parsed JSON data
                return response.json()
            elif response.status_code == 429:  # CoinGecko rate limit code (adjust if needed)
                retry_after = int(response.headers.get('Retry-After', 30))
                print(f"Hit rate limit. Retrying after {retry_after} seconds")
                time.sleep(retry_after)
                # Recursive call to retry after waiting
                return self.fetch_token_data(vs_currency, per_page, page)
            else:
                print(f"API Error. Status Code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            # Catch potential network errors
            print(f"Error fetching data: {e}")
            return None

    def save_token_list(self, db_file="token_data.db"):
        # Fetches a list of all tokens and updates the database
        list_endpoint = "coins/list"
        response = requests.get(self.base_url + list_endpoint)

        if response.status_code == 200:
            token_list = response.json()
        else:
            print("Error fetching token list.")
            return

        conn = sqlite3.connect(db_file)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        ''')

        existing_token_ids = {token[0] for token in self.get_token_list()}

        for token in token_list:
            if token['id'] not in existing_token_ids:
                try:
                    conn.execute("INSERT INTO tokens VALUES (?, ?)", (token['id'], token['name']))
                except sqlite3.IntegrityError:
                    pass  # Ignore duplicate IDs

        conn.commit()
        conn.close()
        print("Token list updated.")

    @staticmethod
    def get_token_list(db_file="token_data.db"):

        conn = sqlite3.connect(db_file)
        tokens = conn.execute("SELECT id FROM tokens").fetchall()

        conn.commit()
        conn.close()

        return tokens

    def check_and_update_token_list(self, db_file="token_data.db"):
        # 1. Fetch Token List from CoinGecko
        list_endpoint = "coins/list"
        response = requests.get(self.base_url + list_endpoint)

        if response.status_code == 200:
            token_list = response.json()
        else:
            print("Error fetching token list.")
            return  # Exit on error

        # 2. Compare with Database
        conn = sqlite3.connect(db_file)
        existing_token_ids = {token[0] for token in self.get_token_list()}
        conn.close()

        # 3. Update 'tokens' Table
        conn = sqlite3.connect(db_file)
        for token in token_list:
            if token['id'] not in existing_token_ids:
                try:
                    conn.execute("INSERT INTO tokens VALUES (?, ?)", (token['id'], token['name']))
                except sqlite3.IntegrityError:
                    pass  # Handle potential ID duplicates gracefully
        conn.commit()
        conn.close()

        print("Token list updated.")

    def fetch_token_volume(self, coin_id):
        # Fetches token data from CoinGecko, handling errors and rate limits
        endpoint = f"coins/{coin_id}"

        try:
            response = requests.get(self.base_url + endpoint)

            if response.status_code == 200:

                # Successful response, return parsed JSON data
                data = response.json()

                if not data['market_data']['total_volume']:
                    daily_volume = "null"
                else:
                    daily_volume = data['market_data']['total_volume']['usd']

                print(f"{coin_id}: {daily_volume}")

                return daily_volume

            elif response.status_code == 429:  # CoinGecko rate limit code (adjust if needed)
                retry_after = int(response.headers.get('Retry-After', 30))
                print(f"Hit rate limit. Retrying after {retry_after} seconds")
                time.sleep(retry_after)
                # Recursive call to retry after waiting
                return self.fetch_token_volume(coin_id)
            else:
                print(f"API Error. Status Code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            # Catch potential network errors
            print(f"Error fetching data: {e}")
            return None

    def save_token_volumes(self, db="token_data.db"):

        conn = sqlite3.connect(db)

        # Retrieves token volumes with pagination, respecting rate limits
        all_token_data = self.get_token_list()

        if not len(all_token_data):
            print("No tokens found in the token list. Skipping saving volumes")
            return

        for token in all_token_data:
            token_id = str(token[0])
            volume = self.fetch_token_volume(token_id)
            try:
                conn.execute("""
                    INSERT INTO token_volumes (token_id, total_volume) 
                    VALUES (?, ?)
                """, (token_id, volume))
            except sqlite3.IntegrityError as e:
                print(f"Database integrity error for {token_id}: {e}")
            except sqlite3.OperationalError as e:
                print(f"Database operational error for {token_id}: {e}")

        conn.commit()
        conn.close()
        response = "Token volume data saved."

        return response

    @staticmethod
    def get_token_volume(db_file="token_data.db"):
        conn = sqlite3.connect(db_file)
        token_volume = conn.execute("SELECT id FROM token_volumes").fetchall()

        conn.commit()
        conn.close()

        return token_volume

    def update_token_volumes(self, db_file="token_data.db"):

        tokens = self.get_token_volume()

        if not tokens:
            return  # Exit if no data

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 2. Update Existing Records within 'token_volumes' Table
        for token in tokens:

            # 1. Fetch Token Volumes from CoinGecko
            token_volume = self.fetch_token_volume(token['id'])

            try:
                conn.execute("""
                    UPDATE token_volumes 
                    SET total_volume = ?
                    WHERE token_id = ? AND date = date('now')
                """, (token['total_volume'], token_volume))

                # If no rows updated, insert new entry (token might be new for today)
                if conn.total_changes == 0:
                    conn.execute("""
                        INSERT INTO token_volumes (token_id, total_volume) 
                        VALUES (?, ?)
                    """, (token['id'], token_volume))

                # Fetch yesterday's volume for comparison
                cursor.execute("""
                       SELECT total_volume 
                       FROM token_volumes 
                       WHERE token_id = ? AND date = date('now', '-1 day')
                   """, (token['id'],))

                result = cursor.fetchall()
                yesterdays_volume = result[0][0] if result else 0  # Handle if no record for yesterday

                print(yesterdays_volume)

                if token_volume > yesterdays_volume * 2:
                    print("volume is 2x lol")

            except sqlite3.IntegrityError as e:
                print(f"Database integrity error for {token['id']}: {e}")
            except sqlite3.OperationalError as e:
                print(f"Database operational error for {token['id']}: {e}")

        conn.commit()
        conn.close()

        print("Token volume data updated.")  # Change message, as 'saved' isn't entirely accurate anymore

    @staticmethod
    def delete_old_data(db_file='token_data.db'):
        conn = sqlite3.connect(db_file)

        # Careful with the date calculation! Adjust for timezones if needed.
        conn.execute(""" 
            DELETE FROM token_volumes 
            WHERE date < date('now', '-21 days') 
        """)

        conn.commit()
        conn.close()

        response = "Token volume data deleted."

        print(response)

    def start_scheduler(self):
        schedule.every().hour.do(self.update_token_volumes)
        schedule.every().hour.do(self.check_and_update_token_list)

        # Add data cleanup:
        schedule.every().day.at("00:01").do(self.delete_old_data)  # Run cleanup at 1 minute past midnight daily

        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    api = CoinGeckoAPI()
    api.save_token_list()  # Update token list if needed
    api.save_token_volumes()  # Fetch and save volumes
