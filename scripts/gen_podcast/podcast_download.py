import argparse
import requests
import hashlib
import time
import json
import pandas as pd
import os
import shutil
from tqdm import tqdm
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime

# Add argument parsing
parser = argparse.ArgumentParser(description='Download podcast episodes.')
parser.add_argument('--ai_episode_count', type=int, default=30, help='Number of episodes to download for AI podcasts.')
parser.add_argument('--non_ai_episode_count', type=int, default=15, help='Number of episodes to download for non-AI podcasts.')
args = parser.parse_args()

# Define your strings
user_agent = "erniesg"

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

# import requests
# import hashlib
# import time
# import json
# import pandas as pd
# import os
# import shutil
# from tqdm import tqdm
# from dotenv import load_dotenv
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
# import os
# from datetime import datetime

# # Define your strings
# user_agent = "erniesg"

# # Load the .env file
# load_dotenv()

# # Get the Key Vault URL from an environment variable
# KEY_VAULT_URL = os.getenv('KEY_VAULT_URL')

# # Create a credential object using the DefaultAzureCredential class
# credential = DefaultAzureCredential()

# # Create a SecretClient object
# secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# # Retrieve the secrets
# api_key = secret_client.get_secret("podcast-api").value
# api_secret = secret_client.get_secret("podcast-secret").value

# # Get the current UTC Unix epoch time
# x_auth_date = str(int(time.time()))

# # Concatenate the strings
# concatenated_string = api_key + api_secret + x_auth_date

# # Create a SHA-1 hash object
# hash_object = hashlib.sha1(concatenated_string.encode())

# # Get the hexadecimal representation of the hash
# hex_dig = hash_object.hexdigest()

# headers = {
#     "User-Agent": user_agent,
#     "X-Auth-Key": api_key,
#     "X-Auth-Date": x_auth_date,
#     "Authorization": hex_dig,
# }

# podcasts = [
#     "The Ezra Klein Show",
#     "Practical AI: Machine Learning, Data Science",
#     "Hard Fork | The New York Times",
#     "Exponent | Ben Thompson",
#     "In Machines We Trust",
#     "Lex Friedman Podcast",
#     "Eye on AI",
#     "Data Decade",
#     "Freakonomics Radio",
#     "The Lunar Society | Dwarkesh Patel",
#     "Azeem Azhar's Exponential View",
#     "AI Today Podcast: Artificial Intelligence Insights, Experts, and Opinion, AI & Data Today",
#     "The AI Breakdown: Daily Artificial Intelligence News and Discussions, Nathaniel Whittemore",
#     "The TWIML AI Podcast (formerly This Week in Machine Learning & Artificial Intelligence), Sam Charrington",
#     "AI with AI: Artificial Intelligence with Andy Ilachinski, CNA",
#     "No Priors: Artificial Intelligence | Machine Learning | Technology | Startups, Conviction | Pod People",
#     "Artificial Intelligence 101 | Luca Marchesotti",
#     "AI Quick Bits: Snackable Artificial Intelligence Content for Everyone, Scot Pansing"
# ]

# base_url = "https://api.podcastindex.org/api/1.0/search/byterm?q="

# # Initialize an empty list to store the results
# results = []
# episodes = []

# non_ai_podcasts = ["The Ezra Klein Show", "Lex Friedman Podcast", "Freakonomics Radio", "Exponent | Ben Thompson", "Hard Fork | The New York Times", "Azeem Azhar's Exponential View", "The Lunar Society | Dwarkesh Patel"]

# for podcast in podcasts:
#     # URL encode the podcast to ensure it can be passed as a parameter in the URL
#     encoded_podcast = requests.utils.quote(podcast)
#     response = requests.get(base_url + encoded_podcast, headers=headers)

#     # the response will be a JSON that you can convert to a dictionary using .json()
#     data = response.json()

#     # Check if the 'count' key in the response is >1
#     if data['count'] > 1:
#         # Add only the first feed
#         feed = data['feeds'][0]
#     elif data['count'] == 1:
#         # Add the only feed available
#         feed = data['feeds'][0]
#     else:
#         continue

#     result = {
#         'id': feed['id'],
#         'title': feed['title'],
#         'url': feed['url'],
#         'originalUrl': feed['originalUrl'],
#         'link': feed['link'],
#         'description': feed['description'],
#         'author': feed['author'],
#         'podcastGuid': feed['podcastGuid'],
#         'ai_only': 0 if podcast in non_ai_podcasts else 1
#     }
#     results.append(result)

#     # Fetch episodes for each feed
#     base_url_episodes = "https://api.podcastindex.org/api/1.0/episodes/byfeedid?id="
#     response_episodes = requests.get(base_url_episodes + str(feed['id']), headers=headers)
#     data_episodes = response_episodes.json()

#     episode_count = 30 if result['ai_only'] else 15
#     data_episodes['items'] = data_episodes['items'][:episode_count]

#     for item in data_episodes['items']:
#         episode = {
#             'feed_id': feed['id'],
#             'feed_title': feed['title'],
#             'id': item.get('id', ''),
#             'title': item.get('title', ''),
#             'link': item.get('link', ''),
#             'description': item.get('description', ''),
#             'guid': item.get('guid', ''),
#             'datePublished': item.get('datePublished', ''),
#             'datePublishedPretty': item.get('datePublishedPretty', ''),
#             'enclosureUrl': item.get('enclosureUrl', ''),
#             'feedLanguage': item.get('feedLanguage', '')
#         }
#         episodes.append(episode)

# # Create a DataFrame from the results
# df = pd.DataFrame(results)
# df

# df_episodes = pd.DataFrame(episodes)
# df_episodes

# # Note: to update this to fetch the latest downloaded_episodes.csv
# try:
#     df_downloaded = pd.read_csv('../raw_data/csv/downloaded_episodes.csv')
# except FileNotFoundError:
#     df_downloaded = pd.DataFrame(columns=['feed_id', 'feed_title', 'id', 'title', 'link', 'description', 'guid', 'datePublished', 'datePublishedPretty', 'enclosureUrl', 'feedLanguage', 'downloaded', 'filepath', 'filename'])
#     df_downloaded['downloaded'] = 0
#     df_downloaded['filepath'] = ''
#     df_downloaded['filename'] = ''

# # Define the base directory for the downloads
# base_dir = "../raw_data/mp3"

# # Ensure the base directory exists
# os.makedirs(base_dir, exist_ok=True)

# # Number of episodes that need to be downloaded
# num_to_download = df_episodes[~df_episodes['id'].isin(df_downloaded['id'])].shape[0]

# # Download the media files
# with tqdm(total=num_to_download, desc="Downloading files") as pbar:
#     # Skip downloading existing episodes
#     for idx, row in df_episodes.iterrows():
#         if row['id'] not in df_downloaded['id'].values:
#             # Create a directory for the feed if it does not exist
#             feed_dir = os.path.join(base_dir, row['feed_title'])
#             os.makedirs(feed_dir, exist_ok=True)

#             # Download the media file
#             print(f"Downloading {row['enclosureUrl']} from {row['feed_title']}")

#             try:
#                 response = requests.get(row['enclosureUrl'], stream=True)
#                 if response.status_code == 200:
#                     file_size = int(response.headers.get('Content-Length', 0))
#                     block_size = 10485760  # 10 Megabytes

#                     file_path = os.path.join(feed_dir, f"{row['id']}.mp3")
#                     with open(file_path, 'wb') as f:
#                         for data in response.iter_content(block_size):
#                             f.write(data)

#                     # Print out the file path and name of the saved file
#                     print(f"Filepath: {os.path.dirname(file_path)}")
#                     print(f"Filename: {os.path.basename(file_path)}")

#                     # Append new episode to df_downloaded
#                     df_downloaded = df_downloaded.append(row, ignore_index=True)

#                     # Update the 'downloaded' column
#                     df_downloaded.loc[df_downloaded['id'] == row['id'], 'downloaded'] = 1
#                     # Update the 'filepath' and 'filename' columns with the path and name of the saved file
#                     df_downloaded.loc[df_downloaded['id'] == row['id'], 'filepath'] = os.path.dirname(file_path)
#                     df_downloaded.loc[df_downloaded['id'] == row['id'], 'filename'] = os.path.basename(file_path)
#                 else:
#                     print(f"Failed to download: {row['enclosureUrl']}")
#                     df_episodes.loc[idx, 'downloaded'] = 0
#             except Exception as e:
#                 print(f"An error occurred while downloading: {row['enclosureUrl']}. Error: {e}")
#                 df_episodes.loc[idx, 'downloaded'] = 0

#             pbar.update(1)

# df_downloaded

# # Create CSV directory
# csv_dir = "../raw_data/csv"
# os.makedirs(csv_dir, exist_ok=True)

# # Get today's date in the format ddmmyyyy
# today = datetime.now().strftime('%d%m%Y')

# # Save the DataFrame to a CSV file with today's date in the filename
# df_downloaded.to_csv(os.path.join(csv_dir, f'{today}_downloaded_episodes.csv'))
