import os
import whisper
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# Load the whisper model
model = whisper.load_model("base.en")

# Load the DataFrame from the CSV file (NOTE: Update this to read the latest downloaded_episodes.csv)
df_downloaded = pd.read_csv('../raw_data/csv/10072023_downloaded_episodes.csv')

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
            # Print the mp3 file path
            print(f"MP3 file path: {mp3_file_path}")
            # Check if the file exists
            if os.path.exists(mp3_file_path):
                try:
                    # Transcribe the mp3 file
                    print(f"Processing {row['filename']} of {row['title']}...")
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

                    print(f"Transcribed text: {result['text'][:100]}...")  # Show the first 100 characters of the transcription

                    # Update progress bar
                    pbar.update(1)

                except Exception as e:
                    print(f"Error processing file {row['filename']}: {e}")
            else:
                print(f"File {row['filename']} not found!")

# Get today's date in the format ddmmyyyy
today = datetime.now().strftime('%d%m%Y')

# Save the transcriptions DataFrame to a CSV file with today's date in the filename
df_transcriptions.to_csv(os.path.join("../raw_data/csv", f'{today}_podcast_transcribed.csv'), index=False)

# Print the DataFrame
df_transcriptions
