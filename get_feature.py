
#import libraries
import requests
import pandas as pd
import numpy as np 

import sys
sys.path.append('/Users/minhnguyen/IronHack2023-2024/Bootcamp/')
from config_2 import *

import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials
from time import sleep

#Initialize SpotiPy with user credentias #
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))


# BLOCK A: FUNCTION TO SEARCH SPOTIFY ID FOR A SONG WITH SONG TITLE AND ARTIST

# function search song:
# results = sp.search(q="track:'+Great Gatsby+' artist:'+Rod Wave'", limit=1)
def search_song(title:str, artist:str = None, limit:int = 1) ->str:
    """
    Searches for a song on Spotify based on the given title and optional artist.
    
    Parameters:
    - title (str): The title of the song to search for.
    - artist (str, optional): The artist of the song. If provided, the search is refined
      to match both the title and artist.
    - limit (int, optional): The maximum number of search results to retrieve. Default is 1.

    Returns:
    - str: The Spotify ID of the first matching song found in the search results.

    Note:
    - The function uses the Spotify API to perform the search.
    - If no match is found, an IndexError may occur. It is advisable to handle such cases
      when using this function.
    """
    if artist == None:
        result=sp.search(q=f"track:{title}", limit=limit)
        song_id = result['tracks']['items'][0]['id']
    else:
        result=sp.search(q=f"track:{title} artist:{artist}", limit=limit)
        song_id = result['tracks']['items'][0]['id']
    return song_id

# BLOCK B: FUNCTION TO SPLIT A LIST OR A DATAFRAME INTO SUBSETS OF ~50 ITEMS

# split list of song ids:
def chunks (song_ids, n:int =50)-> list:
    """
    Divides a sequence of song IDs into chunks of a specified size.

    Parameters:
    - song_ids (list or pandas.DataFrame): The sequence of song IDs to be divided into chunks.
      It can be either a list or a pandas DataFrame.
    - n (int, optional): The desired size of each chunk. Default is 50.

    Returns:
    - list: A list containing chunks of song IDs, where each chunk has a maximum size of 'n'.

    Note:
    - If 'song_ids' is a list, the chunks are created using list slicing.
    - If 'song_ids' is a pandas DataFrame, the chunks are created using DataFrame row slicing.
    - If 'song_ids' is smaller than 'n', a single chunk containing all elements is returned.
    """
    if len(song_ids) > n:
        if type(song_ids) == list:
            chunks = [song_ids[x:x+n] for x in range(0, len(song_ids), n)]
            return chunks
        elif type(song_ids) == pd.DataFrame:
            chunks = [song_ids.iloc[x:x+n,] for x in range(0, len(song_ids), n)]
            return chunks
        else:
            pass
        
    else:
        chunks = [song_ids]
        return chunks


# BLOCK C: FUNCTION TO GET THE LIST OF SONG IDS
# getting list of spotify song ids 
def get_list_song_ids(df,col_1:str="Song_title", col_2:str="Artist" ):
    """
    Collects Spotify song IDs for a DataFrame containing song titles and optional artist information.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing song information, including titles and artists.
    - col_1 (str, optional): The column name for song titles. Default is "Song_title".
    - col_2 (str, optional): The column name for artist information. Default is "Artist".

    Returns:
    - tuple: A tuple containing two elements:
        - list: A list of Spotify song IDs collected for the provided DataFrame. Note that None
                may be present in the list for songs that were not found.
        - pandas.DataFrame: A cleaned DataFrame with added 'song_id' column.

    Note:
    - The function uses the 'search_song' function to retrieve Spotify song IDs.
    - It divides the DataFrame into chunks, performs Spotify searches for each chunk, and
      includes a sleep interval to avoid exceeding rate limits.
    - The resulting 'song_ids' list contains only non-None values (successfully found song IDs).
    - The 'clean_hot_song' DataFrame is the original DataFrame with rows containing
      None in the 'song_id' column dropped.
    """
    df2 = df.copy()
    hot_songs_dfs = chunks(df2)
    song_ids = []    
    for index, hot_songs_df in enumerate(hot_songs_dfs):        
        print(f'Collecting spotify song_id for chunk {index}')        
        for i,row in hot_songs_df.iterrows():        
            try:
                # search for spotify id
                song_id = search_song (row[col_1], row[col_2])
                song_ids.append(song_id)                
            except:
                print("Song not found")
                song_ids.append(None)                
        print("sleep a bit before getting the next chunk")  
        sleep(30)
    df2['song_id'] = song_ids
    song_ids_final = [value for value in song_ids if value is not None]
    clean_hot_song = df2.dropna()     
    return song_ids_final, clean_hot_song    

### BLOCK D: AUDIO feature: combine BLOCK 1 and BLOCK 2 in one FUNCTION
# getting audio features function from a list of 50 or 100 song ids:
def get_audio_features (list:list):
    """
    Retrieves audio features from Spotify for a list of song IDs.

    Parameters:
    - list (list): A list of Spotify song IDs for which audio features will be retrieved.

    Returns:
    - tuple: A tuple containing two elements:
        - dict: A dictionary containing audio features for each song ID. Keys include:
                'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'type',
                'id', 'uri', 'track_href', 'analysis_url', 'duration_ms', 'time_signature'.
        - pandas.DataFrame: A DataFrame representing the same audio features in a tabular format.

    Note:
    - The function uses the Spotify API to retrieve audio features for each song ID in the provided list.
    - It divides the list into chunks to avoid exceeding rate limits.
    - The resulting 'audio_features_dict' contains lists of values for each audio feature.
    - 'audio_features_df' is a DataFrame created from 'audio_features_dict' for tabular representation.
    - Rate limiting is handled, and the function waits between chunks to avoid API restrictions.
    """
    sublists = chunks(list,100)
    audio_features_dict ={'danceability':[], 'energy':[], 'key':[], 'loudness':[], 'mode':[], 'speechiness':[], 'acousticness':[],'instrumentalness':[], 'liveness':[], 'valence':[], 'tempo':[], 'type':[], 'id':[], 'uri':[], 'track_href':[], 'analysis_url':[], 'duration_ms':[], 'time_signature':[]}
    for index,list in enumerate(sublists):
        print(f"Retrieving audio_features from chunk {index}")
        # get audio_features
        try:
            audio_features = sp.audio_features(list)
            for feature in audio_features:
                for key in audio_features_dict:
                    audio_features_dict[key].append(feature[key])
            #audio_features['song_id'] = song_id # add dict item with key 'song_id' and value song_id            
            #audio_features_list.append(audio_features)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 1))
                print(f"Rate limited. Retrying after {retry_after} seconds.")
                sleep(retry_after + 1)
                continue
            else:
                raise
        except Exception as e:
            print(f"Failed to get audio features for some track IDs: {e}")

        print("sleep a bit before getting the next chunk")  
        sleep(60)

    audio_features_df = pd.DataFrame(audio_features_dict)
    return audio_features_dict, audio_features_df

### BLOCK E: FUNCTION TO COMBINE DATAFRAME HOT SONG WITH DATAFRAME OF AUDIO FEATURES

# function to add audio features to song_name, artist df:
def add_audio_features (df1, df2, left_col, right_col, how = 'inner' ):
    """
    Adds audio features from one DataFrame to another based on specified columns.

    Parameters:
    - df1 (pandas.DataFrame): The left DataFrame to which audio features will be added.
    - df2 (pandas.DataFrame): The right DataFrame containing audio features to be added.
    - left_col (str): The column in df1 used for merging.
    - right_col (str): The column in df2 used for merging.

    Returns:
    - pandas.DataFrame: A new DataFrame resulting from the merge of df1 and df2 based on the specified columns.

    Note:
    - The function uses pandas' merge function to combine the two DataFrames.
    - 'left_col' and 'right_col' are used as the merging keys.
    - The resulting 'extended_df' DataFrame contains all columns from both DataFrames.
    """
    extended_df = pd.merge(df1, df2, left_on=left_col, right_on=right_col, how = how)
    return extended_df
