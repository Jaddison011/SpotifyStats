import sqlite3, codecs, json

global c
global conn


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
            c.execute('SELECT SongName, Artist, AlbumName FROM SongIDs WHERE SongName = ? AND Artist = ?',
                      (song["master_metadata_track_name"], song["master_metadata_album_artist_name"]))
            existing_song = c.fetchall()
            # print(existing_song)

            if song["ms_played"] >= 50000:
                if not existing_song:
                    # New song
                    c.execute('INSERT INTO SongIDs VALUES (?, ?, ?, ?)',
                              (current_id, song["master_metadata_album_artist_name"],
                               song["master_metadata_track_name"], song["master_metadata_album_album_name"]))
                    conn.commit()

                    c.execute('INSERT INTO StreamingHistory VALUES (?, ?, ?)',
                              (current_id, song["ts"], song["ms_played"]))
                    conn.commit()
                    current_id += 1
                else:
                    # Existing song
                    print("Not new song: " + song["master_metadata_track_name"])
                    c.execute('SELECT SongID FROM SongIDs WHERE SongName = ? AND Artist = ? AND AlbumName = ?',
                              (song["master_metadata_album_artist_name"],
                               song["master_metadata_track_name"], song["master_metadata_album_album_name"]))
                    song_id = c.fetchone()[0]

                    c.execute('INSERT INTO StreamingHistory VALUES (?, ?, ?)',
                              (song_id, song["ts"], song["ms_played"]))
                    conn.commit()


if __name__ == '__main__':
    conn = sqlite3.connect('SpotifyStats.db')
    c = conn.cursor()
    create_new_table()
    load_all_time_stats('endsong_0.json')
    # load_all_time_stats('endsong_0.json', 'endsong_1.json', 'endsong_2.json', 'endsong_3.json',
    #            'endsong_4.json', 'endsong_5.json', 'endsong_6.json', 'endsong_7.json', 'endsong_8.json')

    c.execute('SELECT * FROM SongIDs')
    print(c.fetchall())

    # c.execute('SELECT * FROM StreamingHistory')
    # print(c.fetchall())
