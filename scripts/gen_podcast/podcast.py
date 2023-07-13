import argparse
import requests
import hashlib
import time
import pandas as pd
import os
from tqdm import tqdm
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
import whisper

def load_secrets():
    # Load the .env file
    load_dotenv()

    # Get the Key Vault URL from an environment variable
    KEY_VAULT_URL = os.getenv('KEY_VAULT_URL')

    # Create a credential object using the DefaultAzureCredential class
    credential = DefaultAzureCredential()

    # Create a SecretClient object
    secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

    # Retrieve the secrets
    api_key = secret_client.get_secret("podcast-api").value
    api_secret = secret_client.get_secret("podcast-secret").value

    return api_key, api_secret

def download_podcasts(api_key, api_secret):
    # Define your strings
    user_agent = "erniesg"

    # Get the current UTC Unix epoch time
    x_auth_date = str(int(time.time()))

    # Concatenate the strings
    concatenated_string = api_key + api_secret + x_auth_date

    # Create a SHA-1 hash object
    hash_object = hashlib.sha1(concatenated_string.encode())

    # Get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()

    headers = {
        "User-Agent": user_agent,
        "X-Auth-Key": api_key,
        "X-Auth-Date": x_auth_date,
        "Authorization": hex_dig,
    }


    podcasts = [
        "The Ezra Klein Show",
        "Practical AI: Machine Learning, Data Science",
        "Hard Fork | The New York Times",
        "Exponent | Ben Thompson",
        "In Machines We Trust",
        "Lex Friedman Podcast",
        "Eye on AI",
        "Data Decade",
        "Freakonomics Radio",
        "The Lunar Society | Dwarkesh Patel",
        "Azeem Azhar's Exponential View",
        "AI Today Podcast: Artificial Intelligence Insights, Experts, and Opinion, AI & Data Today",
        "The AI Breakdown: Daily Artificial Intelligence News and Discussions, Nathaniel Whittemore",
        "The TWIML AI Podcast (formerly This Week in Machine Learning & Artificial Intelligence), Sam Charrington",
        "AI with AI: Artificial Intelligence with Andy Ilachinski, CNA",
        "No Priors: Artificial Intelligence | Machine Learning | Technology | Startups, Conviction | Pod People",
        "Artificial Intelligence 101 | Luca Marchesotti",
        "AI Quick Bits: Snackable Artificial Intelligence Content for Everyone, Scot Pansing"
    ]

    base_url = "https://api.podcastindex.org/api/1.0/search/byterm?q="

    # Initialize an empty list to store the results
    results = []
    episodes = []

    non_ai_podcasts = ["The Ezra Klein Show", "Lex Friedman Podcast", "Freakonomics Radio", "Exponent | Ben Thompson", "Hard Fork | The New York Times", "Azeem Azhar's Exponential View", "The Lunar Society | Dwarkesh Patel"]

    for podcast in podcasts:
        # URL encode the podcast to ensure it can be passed as a parameter in the URL
        encoded_podcast = requests.utils.quote(podcast)
        response = requests.get(base_url + encoded_podcast, headers=headers)

        # the response will be a JSON that you can convert to a dictionary using .json()
        data = response.json()

        # Check if the 'count' key in the response is >1
        if data['count'] > 1:
            # Add only the first feed
            feed = data['feeds'][0]
        elif data['count'] == 1:
            # Add the only feed available
            feed = data['feeds'][0]
        else:
            continue

        result = {
            'id': feed['id'],
            'title': feed['title'],
            'url': feed['url'],
            'originalUrl': feed['originalUrl'],
            'link': feed['link'],
            'description': feed['description'],
            'author': feed['author'],
            'podcastGuid': feed['podcastGuid'],
            'ai_only': 0 if podcast in non_ai_podcasts else 1
        }
        results.append(result)

        # Fetch episodes for each feed
        base_url_episodes = "https://api.podcastindex.org/api/1.0/episodes/byfeedid?id="
        response_episodes = requests.get(base_url_episodes + str(feed['id']), headers=headers)
        data_episodes = response_episodes.json()

        episode_count = args.ai_episode_count if result['ai_only'] else args.non_ai_episode_count
        data_episodes['items'] = data_episodes['items'][:episode_count]

        for item in data_episodes['items']:
            episode = {
                'feed_id': feed['id'],
                'feed_title': feed['title'],
                'id': item.get('id', ''),
                'title': item.get('title', ''),
                'link': item.get('link', ''),
                'description': item.get('description', ''),
                'guid': item.get('guid', ''),
                'datePublished': item.get('datePublished', ''),
                'datePublishedPretty': item.get('datePublishedPretty', ''),
                'enclosureUrl': item.get('enclosureUrl', ''),
                'feedLanguage': item.get('feedLanguage', '')
            }
            episodes.append(episode)

    # ... rest of your code ...
    # Create a DataFrame from the results
    df = pd.DataFrame(results)
    df

    df_episodes = pd.DataFrame(episodes)
    df_episodes

    # Note: to update this to fetch the latest downloaded_episodes.csv
    try:
        df_downloaded = pd.read_csv('../raw_data/csv/downloaded_episodes.csv')
    except FileNotFoundError:
        df_downloaded = pd.DataFrame(columns=['feed_id', 'feed_title', 'id', 'title', 'link', 'description', 'guid', 'datePublished', 'datePublishedPretty', 'enclosureUrl', 'feedLanguage', 'downloaded', 'filepath', 'filename'])
        df_downloaded['downloaded'] = 0
        df_downloaded['filepath'] = ''
        df_downloaded['filename'] = ''

    # Define the base directory for the downloads
    base_dir = "../raw_data/mp3"

    # Ensure the base directory exists
    os.makedirs(base_dir, exist_ok=True)

    # Number of episodes that need to be downloaded
    num_to_download = df_episodes[~df_episodes['id'].isin(df_downloaded['id'])].shape[0]

    # Download the media files
    with tqdm(total=num_to_download, desc="Downloading files") as pbar:
        # Skip downloading existing episodes
        for idx, row in df_episodes.iterrows():
            if row['id'] not in df_downloaded['id'].values:
                # Create a directory for the feed if it does not exist
                feed_dir = os.path.join(base_dir, row['feed_title'])
                os.makedirs(feed_dir, exist_ok=True)

                # Download the media file
                print(f"Downloading {row['enclosureUrl']} from {row['feed_title']}")

                try:
                    response = requests.get(row['enclosureUrl'], stream=True)
                    if response.status_code == 200:
                        file_size = int(response.headers.get('Content-Length', 0))
                        block_size = 10485760  # 10 Megabytes

                        file_path = os.path.join(feed_dir, f"{row['id']}.mp3")
                        with open(file_path, 'wb') as f:
                            for data in response.iter_content(block_size):
                                f.write(data)

                        # Print out the file path and name of the saved file
                        print(f"Filepath: {os.path.dirname(file_path)}")
                        print(f"Filename: {os.path.basename(file_path)}")

                        # Append new episode to df_downloaded
                        df_downloaded = df_downloaded.append(row, ignore_index=True)

                        # Update the 'downloaded' column
                        df_downloaded.loc[df_downloaded['id'] == row['id'], 'downloaded'] = 1
                        # Update the 'filepath' and 'filename' columns with the path and name of the saved file
                        df_downloaded.loc[df_downloaded['id'] == row['id'], 'filepath'] = os.path.dirname(file_path)
                        df_downloaded.loc[df_downloaded['id'] == row['id'], 'filename'] = os.path.basename(file_path)
                    else:
                        print(f"Failed to download: {row['enclosureUrl']}")
                        df_episodes.loc[idx, 'downloaded'] = 0
                except Exception as e:
                    print(f"An error occurred while downloading: {row['enclosureUrl']}. Error: {e}")
                    df_episodes.loc[idx, 'downloaded'] = 0

                pbar.update(1)

    df_downloaded

    # Create CSV directory
    csv_dir = "../raw_data/csv"
    os.makedirs(csv_dir, exist_ok=True)

    # Get today's date in the format ddmmyyyy
    today = datetime.now().strftime('%d%m%Y')

    # Save the DataFrame to a CSV file with today's date in the filename
    df_downloaded.to_csv(os.path.join(csv_dir, f'{today}_downloaded_episodes.csv'))

    # Add a 'transcribed' column if it doesn't exist
    if 'transcribed' not in df_downloaded.columns:
        df_downloaded['transcribed'] = 0

    # Check for existing transcriptions
    with tqdm(total=df_downloaded.shape[0], desc="Checking files") as pbar:
        for idx, row in df_downloaded.iterrows():
            # Check if filename is not NaN
            if pd.notnull(row['filename']):
                # Check if filename is a string
                if isinstance(row['filename'], str):
                    # Construct the path to the csv and txt files
                    base_filename = os.path.splitext(row['filename'])[0]  # Filename without .mp3
                    csv_file_path = os.path.join(row['filepath'], "csv", f"{base_filename}.csv")
                    txt_file_path = os.path.join(row['filepath'], "csv", f"{base_filename}.txt")

                    # Check if both files exist
                    if os.path.exists(csv_file_path) and os.path.exists(txt_file_path):
                        df_downloaded.loc[df_downloaded['id'] == row['id'], 'transcribed'] = 1
                else:
                    print(f"Unexpected filename value at index {idx}: {row['filename']}")
            else:
                print(f"NaN filename at index {idx}")

            # Update progress bar
            pbar.update(1)

    # Save the DataFrame to a CSV file with today's date in the filename
    df_downloaded.to_csv(os.path.join(csv_dir, f'{today}_downloaded_episodes_w_transcribed.csv'), index=False)

    # Print the DataFrame
    df_downloaded

    # Return the DataFrame
    return df_downloaded

def transcribe_podcasts():
    # Load the whisper model
    model = whisper.load_model("base.en")

    # Load the DataFrame from the CSV file (NOTE: Update this to read the latest downloaded_episodes.csv)
    today = datetime.now().strftime('%d%m%Y')
    df_downloaded = pd.read_csv(f'../raw_data/csv/{today}_downloaded_episodes_w_transcribed.csv')

    # Filter for only downloaded episodes
    df_downloaded = df_downloaded[df_downloaded['downloaded'] == 1]

    # Load transcribed DataFrame or initialize a new one
    try:
        df_transcriptions = pd.read_csv('../raw_data/csv/podcast_transcribed.csv')
    except FileNotFoundError:
        df_transcriptions = pd.DataFrame(columns=['feed_id', 'feed_title', 'id', 'title', 'description', 'datePublished', 'datePublishedPretty', 'filepath', 'transcription', 'transcription_filepath', 'filename', 'transcribed'])

    # Number of new episodes to transcribe
    num_to_transcribe = df_downloaded[~df_downloaded['id'].isin(df_transcriptions['id'])].shape[0]

    # Cycle through all rows in df_downloaded and transcribe each mp3 file
    with tqdm(total=num_to_transcribe, desc="Transcribing files") as pbar:
        for idx, row in df_downloaded.iterrows():
            # Check if the episode is already transcribed
            if row['id'] not in df_transcriptions['id'].values and row['transcribed'] != 1:
                # Construct the path to the mp3 file
                mp3_file_path = os.path.join(row['filepath'], row['filename'])

                # Check if the file exists
                if os.path.exists(mp3_file_path):
                    try:
                        # Transcribe the mp3 file
                        result = model.transcribe(mp3_file_path)

                        # Append new row to the DataFrame
                        new_row = row.to_dict()
                        new_row['transcription'] = result['text']
                        new_row['transcribed'] = 1

                        # Create a directory for the transcriptions if it does not exist
                        transcription_dir = os.path.join("../raw_data/csv", row['feed_title'])
                        os.makedirs(transcription_dir, exist_ok=True)

                        # Write the transcription to a .csv and .txt files
                        base_filename = os.path.splitext(row['filename'])[0]  # Filename without .mp3
                        csv_file_path = os.path.join(transcription_dir, f"{base_filename}.csv")
                        txt_file_path = os.path.join(transcription_dir, f"{base_filename}.txt")
                        transcription_df = pd.DataFrame([result['text']], columns=['transcription'])
                        transcription_df.to_csv(csv_file_path, index=False)
                        with open(txt_file_path, 'w') as f:
                            f.write(result['text'])

                        # Update the 'transcription_filepath' column
                        new_row['transcription_filepath'] = transcription_dir

                        # Append the new row to df_transcriptions
                        df_transcriptions = df_transcriptions.append(new_row, ignore_index=True)

                        # Update progress bar
                        pbar.update(1)

                    except Exception as e:
                        print(f"Error processing file {row['filename']}: {e}")
                else:
                    print(f"File {row['filename']} not found!")

    # Save the transcriptions DataFrame to a CSV file with today's date in the filename
    df_transcriptions.to_csv(os.path.join("../raw_data/csv", f'{today}_podcast_transcribed.csv'), index=False)

    # Print the DataFrame
    print(df_transcriptions)

    # Return the DataFrame
    return df_transcriptions

def main():
    # Load secrets
    api_key, api_secret = load_secrets()

    # Download podcasts
    download_podcasts(api_key, api_secret)

    # Transcribe podcasts
    transcribe_podcasts()

if __name__ == "__main__":
    main()
