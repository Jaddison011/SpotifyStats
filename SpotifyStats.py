import sqlite3, codecs, json

import numpy as np

global c
global conn

# TODO: Make the output formatting better e.g. pretty printed
# TODO: Make graph outputs to include time and a UI
# TODO: e.g ranking of a song or artist over time
# TODO: Make this work with multiple years files (which may have overlapping timestamps)
# TODO: Make this work with both monthly and yearly files - need to reformat both JSON files into one standardised format


def create_new_table():
    c.execute('CREATE TABLE IF NOT EXISTS StreamingHistory (SongID INT, datePlayed TIME)')
    conn.commit()

    c.execute('CREATE TABLE IF NOT EXISTS SongIDs (SongID INT PRIMARY KEY, SongName TEXT, Artist TEXT, AlbumName TEXT)')
    conn.commit()

    c.execute('DELETE FROM StreamingHistory')
    c.execute('DELETE FROM SongIDs')
    conn.commit()


# TODO: unnecessary atm see below
def get_current_max_id():
    # c.execute('SELECT max(SongID) FROM SongIDs')
    # rtrn_val = c.fetchall()[0]
    # if rtrn_val:
    #     return rtrn_val
    return 1


# TODO: Make this use a folder
# TODO: Create a function to convert the all-time and monthly files to the same JSON format
# TODO: populate these variables from the db but for now assume that every streaming_history is provided but should be extended to be so you can just add to the db instead of starting from scratch everytime
def load_stats(*filenames):
    # Note: need to load from the db to populate these variables with already seen songs and song_ids
    known_songs = set()  # set (so O(1) access) of a tuple (song_name, artist_name). Note: ignoring album in order to count singles but can mess up for songs with the same name by the same artist e.g. The 1975 - The 1975
    song_ids_to_names = dict()  # id : (artist_name, song_name, album). Take the first seen album for a song.
    song_names_to_ids = dict()  # (artist_name, song_name) : id
    current_max_id = get_current_max_id()  # 1 by default, assign to current highest in db
    played_songs = []  # list of (id, timestamp). To be returned and added to the db

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


def pretty_print_song_count(entry, position):
    print(str(position) + " | Song: " + str(entry[0]) + " - " + str(entry[1])
          + ", Times Played: " + str(entry[2]))


def pretty_print_song_counts(song_list, limit):
    print("----------------" + " Top " + str(limit) + " Songs " + "----------------")
    position = 1
    for entry in song_list[0:limit]:
        pretty_print_song_count(entry, position)
        position += 1


if __name__ == '__main__':
    conn = sqlite3.connect('SpotifyStats.db')
    c = conn.cursor()
    create_new_table()
    # load_all_time_stats('endsong_0.json')
    played_songs_out, song_dictionary = load_stats('Streaming_History_Audio_2019_0.json', 'Streaming_History_Audio_2019-2020_1.json', 'Streaming_History_Audio_2020-2021_3.json', 'Streaming_History_Audio_2020_2.json',
               'Streaming_History_Audio_2021-2022_6.json', 'Streaming_History_Audio_2021_4.json', 'Streaming_History_Audio_2021_5.json', 'Streaming_History_Audio_2022-2023_8.json', 'Streaming_History_Audio_2022_7.json', 'Streaming_History_Audio_2023_9.json', 'Streaming_History_Audio_2023_10.json')

    save_to_db(played_songs_out, song_dictionary)

    # Get the ids of the top 10 played songs
    # c.execute(
    #     'SELECT SongID, COUNT(*) as `plays` FROM StreamingHistory GROUP BY SongID ORDER BY `plays` DESC LIMIT 10')
    # print(c.fetchall())

    # Get the top 10 played songs
    limit = 100
    c.execute(
        'SELECT SongName, Artist, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY SongIDs.SongID ORDER BY `plays` DESC LIMIT ?', (limit,))
    pretty_print_song_counts(c.fetchall(), limit)


    c.execute(
        'SELECT SongName, Artist, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY SongIDs.SongID ORDER BY `plays` DESC LIMIT ?', (limit,))
    output = c.fetchall()
    print(output)
    print(len(list(filter(lambda x: x[1] == "Chelsea Cutler", output))))

    # Get the top 10 played artists
    c.execute('SELECT Artist, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY Artist ORDER BY `plays` DESC LIMIT 10')
    print(c.fetchall())

    # Get the top 10 played albums
    c.execute(
        'SELECT AlbumName, Artist, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY AlbumName ORDER BY `plays` DESC LIMIT 50')
    print(c.fetchall())

    # c.execute('SELECT * FROM SongIDs DESC LIMIT 50')
    # print(c.fetchall())
