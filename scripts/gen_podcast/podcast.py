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