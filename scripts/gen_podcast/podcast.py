import argparse
from dotenv import load_dotenv
from podcast_download import download_podcasts, load_secrets
from podcast_transcribe import transcribe_podcasts

def main():
    # Load secrets
    api_key, api_secret = load_secrets()

    # Download podcasts
    download_podcasts(api_key, api_secret, args.ai_episode_count, args.non_ai_episode_count)

    # Transcribe podcasts
    transcribe_podcasts()

if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description='Podcast Downloader and Transcriber')
    parser.add_argument('--ai_episode_count', type=int, default=10, help='Number of AI episodes to download.')
    parser.add_argument('--non_ai_episode_count', type=int, default=5, help='Number of non-AI episodes to download.')
    args = parser.parse_args()
    print(args)

    # Load the .env file
    load_dotenv()

    main()

'''
1. Add a way to insert new rows that already exist in .mp3 and .txt but not in the csv
2. Add script to remove old csv that are no longer in use
3. Validate that no. of .mp3 = no. of rows in df_downloaded, ideally no. of mp3 = no. of csv
4. Consider validations/tests such that number of new episodes to be added == number of episodes to be transcribed, ie checked both ways
'''
