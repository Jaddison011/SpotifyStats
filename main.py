import json
from itertools import groupby
import operator

songs = dict()  # (id : times_played)
song_ids = dict()  # (artistName, trackName) : id
song_names = dict()  # id : (artistName, trackName)


def load_files(*filenames):
    current_id = 0
    for filename in filenames:
        file = open(filename, encoding="utf8")
        file_json = json.loads(file.read())
        for song in file_json:
            if song["msPlayed"] >= 30000:
                if not song_ids.__contains__((song["artistName"], song["trackName"])):
                    # New song
                    song_ids[(song["artistName"], song["trackName"])] = current_id
                    song_names[current_id] = (song["artistName"], song["trackName"])
                    songs[current_id] = 1
                    current_id += 1
                elif songs.__contains__(song_ids[(song["artistName"], song["trackName"])]):
                    # Existing song
                    songs[song_ids[(song["artistName"], song["trackName"])]] += 1


def sort_by_highest_plays():
    return sorted(songs.items(), key=lambda y: y[1], reverse=True)


def sort_by_highest_plays_by_artist():
    # artists = [(artist, count)] for each song
    artists = list(map(lambda sid: (song_names[sid[0]][0], sid[1]), sort_by_highest_plays()))
    artists = sorted(artists)
    artist_plays = list()
    for k, g in groupby(artists, operator.itemgetter(0)):
        count = list(map(lambda tuple: tuple[1], list(g)))
        count = sum(count)
        artist_plays.append((k, count))
    return sorted(artist_plays, key=lambda x: x[1], reverse=True)


def pretty_print(sid, position):
    print(str(position) + " | Id: " + str(sid) + ", Song: " + str(song_names[sid][1]) + " - " + str(song_names[sid][0])
          + ", Times Played: " + str(songs[sid]))


def pretty_print_all(song_list):
    position = 1
    for sid in song_list:
        pretty_print(sid[0], position)
        position += 1


def pretty_print_limit(song_list, limit):
    position = 1
    for sid in song_list[0:limit]:
        pretty_print(sid[0], position)
        position += 1


def pretty_print_artist_count(artist_plays):
    position = 1
    for i in artist_plays:
        print(str(position) + " | Artist: " + i[0] + ", Times Played: " + str(i[1]))
        position += 1


def pretty_print_artist_count_limited(artist_plays, limit):
    position = 1
    for i in artist_plays[0:limit]:
        print(str(position) + " | Artist: " + i[0] + ", Times Played: " + str(i[1]))
        position += 1


if __name__ == '__main__':
    load_files('StreamingHistory0 25-12-22.json', 'StreamingHistory1 25-12-22.json', 'StreamingHistory2 25-12-22.json',
               'StreamingHistory3 25-12-22.json', 'StreamingHistory4 25-12-22.json')
    # load_files('testData.json')
    # pretty_print_all(sort_by_highest_plays())
    # pretty_print_limit(sort_by_highest_plays(), 100)
    # pretty_print_artist_count(sort_by_highest_plays_by_artist())
    pretty_print_artist_count_limited(sort_by_highest_plays_by_artist(), 25)

