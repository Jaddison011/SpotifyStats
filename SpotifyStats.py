import sqlite3, codecs, json

import numpy as np

global c
global conn

# songs = dict()  # (id : times_played)
# song_ids = dict()  # (artistName, trackName, album) : id
# song_names = dict()  # id : (artistName, trackName, album)
# played_songs = []  # (id, datePlayed, ms_played)


def create_new_table():
    c.execute('CREATE TABLE IF NOT EXISTS StreamingHistory (SongID INT, datePlayed TIME)')
    conn.commit()

    c.execute('CREATE TABLE IF NOT EXISTS SongIDs (SongID INT PRIMARY KEY, SongName TEXT, Artist TEXT, AlbumName TEXT)')
    conn.commit()

    c.execute('DELETE FROM StreamingHistory')
    c.execute('DELETE FROM SongIDs')
    conn.commit()


# def load_all_time_stats(*filenames):
#     current_id = 0
#     for filename in filenames:
#         file = open(filename, encoding="utf8")
#         file_json = json.loads(file.read())
#         for song in file_json:
#             if song["ms_played"] >= 50000:
#                 if not song_ids.__contains__((song["master_metadata_album_artist_name"],
#                                               song["master_metadata_track_name"],
#                                               song["master_metadata_album_album_name"])):
#                     # New song
#                     song_ids[(song["master_metadata_album_artist_name"], song["master_metadata_track_name"],
#                               song["master_metadata_album_album_name"])] = current_id
#                     song_names[current_id] = (
#                         song["master_metadata_album_artist_name"], song["master_metadata_track_name"],
#                         song["master_metadata_album_album_name"])
#                     played_songs.append((current_id, song["ts"], song["ms_played"]))
#                     current_id += 1
#                 else:
#                     # Existing song
#                     played_songs.append((current_id, song["ts"], song["ms_played"]))


# TODO: Make this use a folder
# TODO: Create a function to convert the all-time and monthly files to the same JSON format
def load_stats(*filenames):
    # Note: need to load from the db to populate these variables with already seen songs and song_ids
    known_songs = set()  # set (so O(1) access) of a tuple (song_name, artist_name). Note: ignoring album in order to count singles but can mess up for songs with the same name by the same artist e.g. The 1975 - The 1975
    song_ids_to_names = dict()  # id : (artist_name, song_name, album). Take the first seen album for a song.
    song_names_to_ids = dict()  # (artist_name, song_name) : id
    current_max_id = 1  # 1 by default, assign to current highest in db

    played_songs = []  # list of (id, timestamp). To be returned and added to the db

    # TODO: populate these variables from the db

    for filename in filenames:
        file = open(filename, encoding="utf8")
        file_json = json.loads(file.read())
        for song in file_json:
            # Only count a song as a play if it was listened for at least 50 seconds
            if song["ms_played"] >= 50000:
                # If the song was NOT seen before add it to the list of known songs and assign an id
                # Add the song_id, the artist_name, song_name, album to a dictionary
                song_name_and_artist = (song["master_metadata_track_name"], song["master_metadata_album_artist_name"])
                if song_name_and_artist not in known_songs:
                    known_songs.add(song_name_and_artist)
                    song_ids_to_names[current_max_id] = (song["master_metadata_track_name"], song["master_metadata_album_artist_name"], song["master_metadata_album_album_name"])
                    song_names_to_ids[song_name_and_artist] = current_max_id
                    current_max_id += 1
                # Regardless of if the song was seen before or not, get the id
                song_id = song_names_to_ids[song_name_and_artist]
                # Add the song_id and timestamp to a database table, add the song_id, artist_name, song_name, album to another database table
                played_songs.append((song_id, song["ts"]))

    return played_songs, song_ids_to_names


def save_to_db(played_songs_input, song_dictionary_input):
    for play in played_songs_input:
        # print(play)
        c.execute('INSERT INTO StreamingHistory VALUES (?, ?)', (play[0], play[1]))
    conn.commit()

    for song_id in song_dictionary_input:
        # print(song_id)
        c.execute('INSERT INTO SongIDs VALUES (?, ?, ?, ?)', (song_id, song_dictionary_input[song_id][0], song_dictionary_input[song_id][1], song_dictionary_input[song_id][2]))
    conn.commit()


if __name__ == '__main__':
    conn = sqlite3.connect('SpotifyStats.db')
    c = conn.cursor()
    create_new_table()
    # load_all_time_stats('endsong_0.json')
    played_songs, song_dictionary = load_stats('endsong_0.json', 'endsong_1.json', 'endsong_2.json', 'endsong_3.json',
               'endsong_4.json', 'endsong_5.json', 'endsong_6.json', 'endsong_7.json', 'endsong_8.json')

    save_to_db(played_songs, song_dictionary)

    # Get the ids of the top 10 played songs
    c.execute(
        'SELECT SongID, COUNT(*) as `plays` FROM StreamingHistory GROUP BY SongID ORDER BY `plays` DESC LIMIT 10')
    print(c.fetchall())
    # [(5831, 297), (4609, 175), (5284, 171), (5441, 161), (5444, 149), (4537, 146), (5042, 141), (4817, 137),
     # (4991, 132), (5573, 127)]

    # Get the top 10 played songs
    c.execute(
        'SELECT SongName, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY SongIDs.SongID ORDER BY `plays` DESC LIMIT 10')
    print(c.fetchall())

    # Get the top 10 played artists
    c.execute('SELECT Artist, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY Artist ORDER BY `plays` DESC LIMIT 10')
    print(c.fetchall())

    # c.execute('SELECT * FROM SongIDs')
    print(c.fetchall())
