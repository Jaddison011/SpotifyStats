import sqlite3, codecs, json

import numpy as np

global c
global conn

# songs = dict()  # (id : times_played)
song_ids = dict()  # (artistName, trackName, album) : id
song_names = dict()  # id : (artistName, trackName, album)
played_songs = []  # (id, datePlayed, ms_played)


def create_new_table():
    c.execute('CREATE TABLE IF NOT EXISTS StreamingHistory (SongID INT, datePlayed TIME, ms_played INTEGER)')
    conn.commit()

    c.execute('CREATE TABLE IF NOT EXISTS SongIDs (SongID INT PRIMARY KEY, SongName TEXT, Artist TEXT, AlbumName TEXT)')
    conn.commit()

    c.execute('DELETE FROM StreamingHistory')
    c.execute('DELETE FROM SongIDs')
    conn.commit()


def load_all_time_stats(*filenames):
    current_id = 0
    for filename in filenames:
        file = open(filename, encoding="utf8")
        file_json = json.loads(file.read())
        for song in file_json:
            if song["ms_played"] >= 50000:
                if not song_ids.__contains__((song["master_metadata_album_artist_name"],
                                              song["master_metadata_track_name"],
                                              song["master_metadata_album_album_name"])):
                    # New song
                    song_ids[(song["master_metadata_album_artist_name"], song["master_metadata_track_name"],
                              song["master_metadata_album_album_name"])] = current_id
                    song_names[current_id] = (
                        song["master_metadata_album_artist_name"], song["master_metadata_track_name"],
                        song["master_metadata_album_album_name"])
                    played_songs.append((current_id, song["ts"], song["ms_played"]))
                    current_id += 1
                else:
                    # Existing song
                    played_songs.append((current_id, song["ts"], song["ms_played"]))


def save_to_db():
    for play in played_songs:
        # print(play)
        c.execute('INSERT INTO StreamingHistory VALUES (?, ?, ?)', (play[0], play[1], play[2]))
    conn.commit()
    for song_id in song_ids:
        # print(song_id)
        c.execute('INSERT INTO SongIDs VALUES (?, ?, ?, ?)', (song_ids[song_id], song_id[1], song_id[0], song_id[2]))
    conn.commit()


if __name__ == '__main__':
    conn = sqlite3.connect('SpotifyStats.db')
    c = conn.cursor()
    create_new_table()
    # load_all_time_stats('endsong_0.json')
    load_all_time_stats('endsong_0.json', 'endsong_1.json', 'endsong_2.json', 'endsong_3.json',
               'endsong_4.json', 'endsong_5.json', 'endsong_6.json', 'endsong_7.json', 'endsong_8.json')

    save_to_db()

    # Get the top 10 played songs
    c.execute(
        'SELECT SongID, COUNT(*) as `plays` FROM StreamingHistory GROUP BY SongID ORDER BY `plays` DESC LIMIT 10')
    print(c.fetchall())
    # [(5831, 297), (4609, 175), (5284, 171), (5441, 161), (5444, 149), (4537, 146), (5042, 141), (4817, 137),
     # (4991, 132), (5573, 127)]

    # Get the top 10 played artists
    # c.execute('SELECT Artist, COUNT(*) as `plays` FROM StreamingHistory JOIN SongIDs WHERE StreamingHistory.SongID == SongIDs.SongID GROUP BY Artist ORDER BY `plays` DESC LIMIT 10')
    # print(c.fetchall())

    c.execute('SELECT * FROM SongIDs')
    print(c.fetchall())
