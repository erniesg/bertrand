import os
import pandas as pd
import requests
import shutil
import torch
import whisper
from datetime import datetime
from tqdm import tqdm

def transcribe_podcasts():
    # Setting up the device
    if torch.cuda.is_available():
        device = torch.device("cuda:0")
        print("Using GPU for computations.")
        fp16 = True  # Enable FP16 when using GPU
    else:
        device = torch.device("cpu")
        print("CUDA not available. Using CPU for computations.")
        fp16 = False  # Disable FP16 when using CPU

    # Load the whisper model
    model = whisper.load_model("base.en").to(device)
    print(f"Is model on GPU: {next(model.parameters()).is_cuda}")

    # Create the base directory
    base_dir = os.getenv('BASE_DIR')
    os.makedirs(base_dir, exist_ok=True)

    # Create the CSV directory within the base directory
    csv_dir = os.path.join(base_dir, 'csv')
    os.makedirs(csv_dir, exist_ok=True)

    # Create the MP3 directory within the base directory
    mp3_dir = os.path.join(base_dir, 'mp3')
    os.makedirs(mp3_dir, exist_ok=True)

    # Get the latest downloaded episodes with transcriptions CSV file
    files = os.listdir(csv_dir)
    latest_file = None

    # Find the latest "ddmmyy_downloaded_episodes_w_transcribed.csv" file
    for file in files:
        if file.endswith("_downloaded_episodes_w_transcribed.csv"):
            if latest_file is None or file > latest_file:
                latest_file = file

    if latest_file is None:
        print("No downloaded episodes with transcriptions CSV file found.")
        return

    downloaded_episodes_csv = os.path.join(csv_dir, latest_file)
    backup_file = downloaded_episodes_csv.replace(".csv", "_backup.csv")

    # Make a copy of the latest downloaded_episodes_w_transcribed.csv file as a backup
    shutil.copy2(downloaded_episodes_csv, backup_file)
    print(f"Created a backup of the latest downloaded episodes with transcriptions CSV file: {backup_file}")

    # Load the DataFrame from the CSV file
    df_downloaded = pd.read_csv(downloaded_episodes_csv)

    # Check if transcript files exist for downloaded episodes
    for idx, row in df_downloaded.iterrows():
        base_filename = os.path.splitext(row['filename'])[0]
        txt_dir_path = os.path.join(csv_dir, row['feed_title'])  # Change csv_dir to txt_dir if you have a separate directory for text files
        txt_file_path = os.path.join(txt_dir_path, f"{base_filename}.txt")

        if os.path.exists(txt_file_path):
            # Set 'transcribed' to 1
            df_downloaded.at[idx, 'transcribed'] = 1

    print(f"Final transcribed dataframe contents:\n{df_downloaded}")

    # Count the number of existing transcriptions
    num_existing_transcriptions = df_downloaded[df_downloaded['transcribed'] == 1].shape[0]

    # Filter for only downloaded episodes that are not transcribed
    df_to_transcribe = df_downloaded[df_downloaded['transcribed'] != 1]

    # Number of new episodes to transcribe
    num_to_transcribe = df_to_transcribe.shape[0]

    print(f"{num_existing_transcriptions} existing transcriptions found. {num_to_transcribe} to be transcribed.")

    # Cycle through episodes to transcribe and transcribe each mp3 file
    with tqdm(total=num_to_transcribe, desc="Transcribing files") as pbar:
        for idx, row in df_to_transcribe.iterrows():
            # Construct the path to the mp3 file
            mp3_file_path = os.path.join(mp3_dir, row['filepath'], row['filename'])

            # Check if the file exists
            if os.path.exists(mp3_file_path):
                try:
                    # Transcribe the mp3 file
                    result = model.transcribe(mp3_file_path, fp16=fp16)
                    # Write the transcription to a .txt files
                    base_filename = os.path.splitext(row['filename'])[0]
                    txt_dir_path = os.path.join(csv_dir, row['feed_title'])  # Change csv_dir to txt_dir if you have a separate directory for text files
                    txt_file_path = os.path.join(txt_dir_path, f"{base_filename}.txt")
                    os.makedirs(txt_dir_path, exist_ok=True)
                    with open(txt_file_path, 'w') as f:
                        f.write(result['text'])
                    # Set 'transcribed' to 1
                    df_downloaded.at[idx, 'transcribed'] = 1

                    # Update progress bar
                    pbar.update(1)

                except Exception as e:
                    print(f"Error transcribing {mp3_file_path}: {str(e)}")
            else:
                print(f"File not found: {mp3_file_path}")

    # Overwrite the original file with the updated DataFrame
    df_downloaded.to_csv(downloaded_episodes_csv, index=False)
    print(f"Updated the original file: {downloaded_episodes_csv}")

    # Return the DataFrame
    return df_downloaded