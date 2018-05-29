import codecs
import sys
import configparser
import time
import sqlite3
import json
from urllib.request import urlopen
from urllib.error import URLError
from bs4 import BeautifulSoup
from shutil import copyfile
from plexapi.server import PlexServer

config_file = 'config.cfg'

with codecs.open(config_file, 'r', 'utf-8') as open_config_file:
    config = configparser.ConfigParser()
    config.read_file(open_config_file)
    settings = config['SETTINGS']


tmdb_api_key = settings['tmdb_api_key']
second_language = settings['tmdb_second_language']

plex_server_ip_address = settings['plex_server_ip_address']
plex_auth_token = settings['plex_auth_token']
plex_server = PlexServer(plex_server_ip_address, plex_auth_token)
library = plex_server.library.section(settings['library_to_modify'])

database_dir = settings['database_dir']
database_backup_dir = settings['database_backup_dir']
database = sqlite3.connect(database_dir)
cursor = database.cursor()


def main():
    pass
    # Backup Database.
    backup_database(database_dir, database_backup_dir)

    # Pick movie in need of update.
    movie = get_movie()

    # Get metadata. (sv_tmdb, en_tmdb, imdb)

    if movie.tmdb_id is None:
        get_tmdb_id(movie)

    get_tmdb_metadata(movie)

    if movie.need_content_rating:
        get_imdb_metadata(movie)

    # Select metadata to commit to database.
    print('proccessing :' + movie.title)
    process_movie(movie)

    # Commit to database.
    commit_to_database(movie)


def commit_to_database(movie):
    def commit_taggs():
        for raw_id, tag_id in movie.movie_tags_list.items():
            cursor.execute('UPDATE  taggings '
                           'SET tag_id = ? '
                           'Where id = ?', (tag_id, raw_id,))

    def commit_metadata():
        cursor.execute('UPDATE metadata_items SET '
                       'title_sort = ?, '
                       'tagline = ?, '
                       'content_rating = ?, '
                       
                       'WHERE id = ?', (movie.title_sort,
                                        movie.tagline,
                                        movie.content_rating,
                                        '|'.join(movie.user_fields),
                                        movie.metadata_id,))

    commit_taggs()
    commit_metadata()


def process_movie(movie):
    def select_new_content_rating():
        raw_ratings = settings['imdb_content_rating_tags'] + ',' + settings['unknown_content_rating']
        raw_ratings.lower()
        for i in range(100):
            raw_ratings.replace(', ', ',').replace(' ,', ',')
        movie.raw_ratings = raw_ratings.split(',')

        combined = dict(zip(movie.raw_ratings, movie.acceptable_ratings))

        if movie.imdb_metadata['content_rating'] in combined:
            movie.contentRating = combined[movie.imdb_metadata['content_rating']]
        else:
            movie.contentRating = '???'

    def select_new_tagline():
        if movie.sv_tmdb_metadata['tagline'] != '':
            movie.tagline = movie.sv_tmdb_metadata['tagline']
        else:
            movie.tagline = movie.en_tmdb_metadata['tagline']

    def select_new_title_sort():
        if movie.sv_tmdb_metadata['title'] == movie.en_tmdb_metadata['title']:
            movie.titleSort = movie.en_tmdb_metadata['title']
        else:
            movie.titleSort = movie.en_tmdb_metadata['title'] + ' ' \
                               + settings['title_sort_delimiter'] + ' '\
                               + movie.sv_tmdb_metadata['title']

    def convert_genres():
        for rename_to, rename_from_list in config.items('GENRE_CHANGES'):
            split_list = rename_from_list.split(',')
            for rename_from in split_list:
                for raw_id in movie.movie_tags_list:

                    if rename_from.lower() not in movie.tag_list:
                        continue

                    elif rename_to.lower() not in movie.tag_list:
                        movie.tag_list[rename_to.lower()] = movie.tag_list.pop(rename_from.lower())

                    elif movie.movie_tags_list[raw_id] == movie.tag_list[rename_from.lower()]:
                        movie.movie_tags_list[raw_id] = movie.tag_list[rename_to.lower()]

    if movie.need_genres:
        convert_genres()

    if movie.need_content_rating:
        select_new_content_rating()

    if movie.need_tagline:
        select_new_tagline()

    if movie.need_title_sort:
        select_new_title_sort()

    # Add collection tag(s).

    # Edit collection.


def backup_database(source_dir, target_dir):

    copyfile(source_dir, target_dir)


def get_movie():

    for movie in library.all():

        movie = library.get('Cars')

        movie.metadata_id = movie.key.split('/')[3]

        if ".themoviedb" in movie.guid:
            movie.tmdb_id = movie.guid.split('//')[1].split('?')[0]
            movie.imdb_id = None
        elif ".imdb" in movie.guid:
            movie.tmdb_id = None
            movie.imdb_id = movie.guid.split('//')[1].split('?')[0]
        else:
            print("Unable find either an imdb-ID or a tmdb-ID for " + movie.title + ", skipping movie.")
            continue

        movie.need_content_rating = False
        movie.need_tagline = False
        movie.need_genres = False
        movie.need_title_sort = False

        acceptable_ratings = settings['renamed_content_ratings'] + ',' + settings['unknown_content_rating']
        for i in range(100):
            acceptable_ratings.replace(', ', ',').replace(' ,', ',')
        movie.acceptable_ratings = acceptable_ratings.split(',')

        if not any(movie.contentRating.lower == s for s in acceptable_ratings):
            movie.need_content_rating = True

        if movie.tagline is None:
            movie.need_tagline = True

        acceptable_genres = list(config['GENRE_CHANGES'].keys())

        for movie_genre in movie.genres:
            for acceptable_genre in acceptable_genres:
                if movie_genre.tag.lower() == acceptable_genre.lower():
                    movie.need_genres = True

        if not settings['title_sort_delimiter'] in movie.titleSort:
            movie.need_title_sort = True

        if not (movie.need_content_rating or movie.need_tagline or movie.need_genres or movie.need_title_sort):
            continue

        tag_list = dict()
        for tag_info in cursor.execute("SELECT id, tag "
                                       "FROM tags "
                                       "WHERE tag_type = 1 "
                                       "ORDER BY id ASC "):
            tag_list[tag_info[1].lower()] = tag_info[0]

        movie.tag_list = tag_list

        movie_tags_list = dict()
        for movie_tags_info in cursor.execute("SELECT id, tag_id "
                                              "FROM taggings "
                                              "WHERE metadata_item_id = ? "
                                              "AND text = ''"
                                              "ORDER BY tag_id ASC ", (movie.metadata_id,)):
            movie_tags_list[movie_tags_info[0]] = movie_tags_info[1]

        movie.movie_tags_list = movie_tags_list

        return movie
    print('didn\'t find any movie')
    sys.exit()


def retrieve_web_page(url, page_name='page'):

    response = None

    for attempt in range(2):
        try:
            response = urlopen(url)
            break
        except URLError:
            print('Failed to download ' + page_name + '. Trying again in 10 seconds')
            time.sleep(10)
            if attempt == 1:
                response = urlopen(url)

    return response


def get_tmdb_id(movie):
    response = retrieve_web_page('https://api.themoviedb.org/3/find/'
                             + movie.imdb_id +
                             '?api_key=' + tmdb_api_key +
                             '&language=en-US'
                             '&external_source=imdb_id', 'tmdb id')

    data = json.loads(response.read().decode('utf-8'))
    response.close()

    movie.tmdb_id = str(data['movie_results'][0]['id'])


def get_tmdb_metadata(movie):

    response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                             + movie.tmdb_id +
                             '?api_key=' + tmdb_api_key +
                             '&language=en-US', 'english movie metadata from tmdb')

    movie.en_tmdb_metadata = json.loads(response.read().decode('utf-8'))
    movie.imdb_id = movie.en_tmdb_metadata['imdb_id']
    response.close()

    if movie.need_title_sort:
        response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                                 + movie.tmdb_id +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=sv-SE', 'english movie metadata from tmdb')
        movie.sv_tmdb_metadata = json.loads(response.read().decode('utf-8'))
        response.close()


def get_imdb_metadata(movie):
    response = retrieve_web_page("https://www.imdb.com/title/" + movie.imdb_id +
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

    if not any(cert in s for s in movie.acceptable_ratings):
        cert = '???' \

    movie.imdb_metadata = {'content_rating': cert}


main()
