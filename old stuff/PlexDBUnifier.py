import codecs
import sys
import configparser
import time
import json
from urllib.request import urlopen
from urllib.error import URLError
from bs4 import BeautifulSoup
from shutil import copyfile
from plexapi.server import PlexServer


def download_page(url, page_name='page'):
    for attempt in range(5):
        if attempt > 2:
            return urlopen(url)
        else:
            try:
                return urlopen(url)
            except URLError:
                print('Failed to download ' + page_name + '. Trying again in 10 seconds')
                time.sleep(10)


def get_movie_metadata_from_db(cursor):

    for movie_info in cursor.execute("SELECT id, title, title_sort, tagline, content_rating, user_fields, guid "
                                     "FROM metadata_items "
                                     "WHERE metadata_type = 1 "
                                     "AND library_section_id = ?"
                                     "ORDER BY RANDOM()", (section,)):

        movie_metadata = dict()
        movie_metadata['metadata_id'] = movie_info[0]
        movie_metadata['title'] = movie_info[1]
        movie_metadata['title_sort'] = movie_info[2]
        movie_metadata['tagline'] = movie_info[3]
        movie_metadata['content_rating'] = movie_info[4]

        movie_metadata['user_fields'] = movie_info[5]
        if movie_metadata['user_fields'] != "":
            movie_metadata['user_fields'] = movie_metadata['user_fields'].split('=')[1]
            movie_metadata['user_fields'] = movie_metadata['user_fields'].split('|')
        else:
            movie_metadata['user_fields'] = list()

        movie_metadata['guid'] = movie_info[6]
        if ".themoviedb" in movie_metadata['guid']:
            movie_metadata['tmdb_id'] = movie_metadata['guid'].split('//')[1].split('?')[0]
            movie_metadata['imdb_id'] = None
        elif ".imdb" in movie_metadata['guid']:
            movie_metadata['imdb_id'] = movie_metadata['guid'].split('//')[1].split('?')[0]
            movie_metadata['tmdb_id'] = None
        else:
            return movie_metadata

        if not (any("2" == s for s in movie_metadata['user_fields']) or "se/" in movie_metadata['content_rating']):
            movie_metadata['title_sort'] = None

        if movie_metadata['tagline'] == "":
            movie_metadata['tagline'] = None

        if not any("8" == s for s in movie_metadata['user_fields']):
            movie_metadata['content_rating'] = None

        if any("15" == s for s in movie_metadata['user_fields']):
            movie_metadata['genre_locked'] = True
        else:
            movie_metadata['genre_locked'] = False

        stat1 = movie_metadata['title_sort'] is not None
        stat2 = movie_metadata['tagline'] is not None
        stat3 = movie_metadata['content_rating'] is not None
        stat4 = movie_metadata['genre_locked']
        if stat1 and stat2 and stat3 and stat4:
            continue

        tag_list = dict()
        for tag_info in cursor.execute("SELECT id, tag "
                                       "FROM tags "
                                       "WHERE tag_type = 1 "
                                       "ORDER BY id ASC "):
            tag_list[tag_info[1].lower()] = tag_info[0]

        movie_metadata['tag_list'] = tag_list

        movie_tags_list = dict()
        for movie_tags_info in cursor.execute("SELECT id, tag_id "
                                              "FROM taggings "
                                              "WHERE metadata_item_id = ? "
                                              "AND text = ''"
                                              "ORDER BY tag_id ASC ", (movie_metadata['metadata_id'],)):
            movie_tags_list[movie_tags_info[0]] = movie_tags_info[1]

        movie_metadata['movie_tags_list'] = movie_tags_list

        return movie_metadata


def get_tmdb_id(tmdb_api_key, imdb_id):
    response = download_page('https://api.themoviedb.org/3/find/'
                             + imdb_id +
                             '?api_key=' + tmdb_api_key +
                             '&language=en-US'
                             '&external_source=imdb_id', 'tmdb id')

    data = json.loads(response.read().decode('utf-8'))
    response.close()

    return str(data['movie_results'][0]['id'])


def get_imdb_id(tmdb_api_key, tmdb_id):

    response = download_page('https://api.themoviedb.org/3/movie/'
                             + tmdb_id +
                             '/external_ids'
                             '?api_key=' + tmdb_api_key, 'imdb id')

    data = json.loads(response.read().decode('utf-8'))
    response.close()

    return data['imdb_id']


def get_tmdb_metadata(movie_metadata, tmdb_api_key):

    response = download_page('https://api.themoviedb.org/3/movie/'
                             + movie_metadata['tmdb_id'] +
                             '?api_key=' + tmdb_api_key +
                             '&language=en-US', 'english movie metadata from tmdb')

    movie_metadata['en_tmdb_metadata'] = json.loads(response.read().decode('utf-8'))
    response.close()

    if movie_metadata['title_sort'] is None:
        response = download_page('https://api.themoviedb.org/3/movie/'
                                 + movie_metadata['tmdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=sv-SE', 'english movie metadata from tmdb')
        movie_metadata['sv_tmdb_metadata'] = json.loads(response.read().decode('utf-8'))
        response.close()

    movie_metadata['imdb_id'] = movie_metadata['en_tmdb_metadata']['imdb_id']

    return movie_metadata


def get_imdb_metadata(imdb_id):
    response = download_page("https://www.imdb.com/title/" + imdb_id +
                             "/parentalguide?ref_=tt_ql_stry_5", 'certification page on imdb')
    data = response.read()
    response.close()
    soup = BeautifulSoup(data)
    cert = str(soup.findAll("a", {"href": lambda l: l and l.startswith('/search/title?certificates=SE:')}))
    try:
        cert = cert.split(':')[1].split('"')[0]
    except IndexError:
        print('The movie don\'t have a rating in this language.')
        cert = "???"
    return {'content_rating': cert}


def select_metadata(movie_metadata, config):
    def select_new_title_sort(mov_metadata):

        if mov_metadata['sv_tmdb_metadata']['title'] == mov_metadata['en_tmdb_metadata']['title']:
            mov_metadata['title_sort'] = mov_metadata['en_tmdb_metadata']['title']
        else:
            mov_metadata['title_sort'] = mov_metadata['en_tmdb_metadata']['title'] + " : " + \
                                         mov_metadata['sv_tmdb_metadata']['title']

        return mov_metadata

    def select_new_content_rating(mov_metadata):

        if 'btl' in mov_metadata['imdb_metadata']['content_rating'].lower():
            mov_metadata['content_rating'] = 'se/Btl'
        elif '7' in mov_metadata['imdb_metadata']['content_rating'].lower():
            mov_metadata['content_rating'] = 'se/7+'
        elif '11' in mov_metadata['imdb_metadata']['content_rating'].lower():
            mov_metadata['content_rating'] = 'se/11+'
        elif '15' in mov_metadata['imdb_metadata']['content_rating'].lower():
            mov_metadata['content_rating'] = 'se/15+'
        else:
            mov_metadata['content_rating'] = '???'
        return mov_metadata

    def select_new_tagline(mov_metadata):

        mov_metadata['tagline'] = mov_metadata['en_tmdb_metadata']['tagline']

        return movie_metadata

    def convert_genres(mov_metadata, genre_settings):
        for rename_to, rename_from_list in genre_settings.items('TAG_CHANGES'):
            split_list = rename_from_list.split(',')
            for rename_from in split_list:
                for raw_id in mov_metadata['movie_tags_list']:

                    if rename_from.lower() not in mov_metadata['tag_list']:
                        continue

                    elif rename_to.lower() not in mov_metadata['tag_list']:
                        mov_metadata['tag_list'][rename_to.lower()] = mov_metadata['tag_list'].pop(rename_from.lower())

                    elif mov_metadata['movie_tags_list'][raw_id] == mov_metadata['tag_list'][rename_from.lower()]:
                        mov_metadata['movie_tags_list'][raw_id] = mov_metadata['tag_list'][rename_to.lower()]

        return mov_metadata

    if movie_metadata['title_sort'] is None:
        movie_metadata = select_new_title_sort(movie_metadata)

    if movie_metadata['content_rating'] is None:
        movie_metadata = select_new_content_rating(movie_metadata)

    if movie_metadata['tagline'] is None:
        movie_metadata = select_new_tagline(movie_metadata)

    if not movie_metadata['genre_locked']:
        movie_metadata = convert_genres(movie_metadata, config)

    return movie_metadata


def backup_database(source_dir, target_dir):
    copyfile(source_dir, target_dir)


def commit_metadata_to_database(movie_metadata, cursor):

    for raw_id, tag_id in movie_metadata['movie_tags_list'].items():
        cursor.execute('UPDATE  taggings '
                       'SET tag_id = ? '
                       'Where id = ?', (tag_id, raw_id,))

    cursor.execute('UPDATE metadata_items SET '
                   'title_sort = ?, '
                   'tagline = ?, '
                   'content_rating = ?, '
                   'user_fields = ? '
                   'WHERE id = ?', (movie_metadata['title_sort'],
                                    movie_metadata['tagline'],
                                    movie_metadata['content_rating'],
                                    '|'.join(movie_metadata['user_fields']),
                                    movie_metadata['metadata_id'],))

    return True


def update_movie_metadata(cursor, tmdb_api_key, config):

    print('Picking a movie to process.')
    movie_metadata = get_movie_metadata_from_db(cursor)
    print(movie_metadata['title'])
    if movie_metadata['tmdb_id'] is None and movie_metadata['imdb_id'] is None:

        print('Failed to retrieve an IMDB id or a TMDB id. exiting')
        return False

    elif movie_metadata['tmdb_id'] is None:

        print('Retrieving TMDB id.')
        movie_metadata['tmdb_id'] = get_tmdb_id(tmdb_api_key, movie_metadata['imdb_id'])

    print('Retrieving TMDB metadata.')
    movie_metadata = get_tmdb_metadata(movie_metadata, tmdb_api_key)

    if movie_metadata['content_rating'] is None:

        print('Retrieving IMDB parental guide certification.')
        movie_metadata['imdb_metadata'] = get_imdb_metadata(movie_metadata['imdb_id'])

    for tag in movie_metadata['movie_tags_list']:
        for tags in movie_metadata['tag_list']:
            if movie_metadata['movie_tags_list'][tag] == movie_metadata['tag_list'][tags]:
                print(tags)

    print('Sorting which metadata to write to the database.')
    movie_metadata = select_metadata(movie_metadata, config)

    for tag in movie_metadata['movie_tags_list']:
        for tags in movie_metadata['tag_list']:
            if movie_metadata['movie_tags_list'][tag] == movie_metadata['tag_list'][tags]:
                print(tags)

    print('Writing metadata to database.')
    return commit_metadata_to_database(movie_metadata, cursor)


config_file = 'config.cfg'

with codecs.open(config_file, 'r', "utf8") as config_open_file:
    conf = configparser.ConfigParser()
    conf.read_file(config_open_file)


api_key = conf.get('SETTINGS', 'tmdb_api_key')
plex_url = conf.get('SETTINGS', 'plex_server_url')
token = conf.get('SETTINGS', 'plex_auth_token')
data

self.database = sqlite3.connect(database_dir)
self.cursor = self.database.cursor()

plex_server = PlexServer(plex_url, token)
section = plex_server.library.section(conf.get('SETTINGS', 'section_to_modify'))

update_movie_metadata(section, api_key, conf)

sys.exit()
