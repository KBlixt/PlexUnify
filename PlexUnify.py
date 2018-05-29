import codecs
import sys
import configparser
import time
import json
from urllib.request import urlopen
from urllib.error import URLError
from shutil import copyfile
from bs4 import BeautifulSoup

# these packages need to be installed:
from plexapi.server import PlexServer
import sqlite3

# global static variable:

# config stuff
config_file = 'config2.cfg'
with codecs.open(config_file, 'r', 'utf-8') as open_config_file:
    config = configparser.ConfigParser()
    config.read_file(open_config_file)
global_settings = config['GLOBAL_SETTINGS']
if global_settings.getboolean('safety_lock', True):
    print('safety_lock is activated! Exiting.')
    sys.exit()

# tmdb stuff
tmdb_api_key = global_settings['tmdb_api_key']
main_language = global_settings['tmdb_main_language']
secondary_language = global_settings['tmdb_secondary_language']

# plex api stuff
plex_server_ip_address = global_settings['plex_server_ip_address']
plex_auth_token = global_settings['plex_auth_token']
plex_server = PlexServer(plex_server_ip_address, plex_auth_token)
library = plex_server.library.section(global_settings['library_to_modify'])
library_key = library.key

# database stuff
database_dir = global_settings['database_dir']
database_backup_dir = global_settings['database_backup_dir']
database = sqlite3.connect(database_dir)
cursor = database.cursor()
main_cursor = database.cursor()
# end of global static variable.

# global variables:

tmdb_movie_metadata = None
secondary_tmdb_movie_metadata = None
tmdb_collection_metadata = None
secondary_tmdb_collection_metadata = None

metadata_items_commits = dict()  #
taggings_commits = dict()        #
tags_commits = dict()            # list of changes to be made on tables. example: 20: {dict of changes}

# end of global variables.


def main():

    global tmdb_movie_metadata
    global secondary_tmdb_movie_metadata
    global tmdb_collection_metadata
    global secondary_tmdb_collection_metadata

    # Backup Database.
    backup_database(database_dir, database_backup_dir)

    for movie_tuple in main_cursor.execute('SELECT id, guid, title, title_sort, tagline, content_rating, user_fields '
                                           'FROM metadata_items '
                                           'WHERE library_section_id = ' + library_key + ' '
                                           'AND metadata_type = 1 '
                                           'ORDER BY title DESC '
                                           'LIMIT ' + str(global_settings.getint('modify_limit'))):

        movie = dict()
        movie['metadata_id'] = movie_tuple[0]
        movie['guid'] = movie_tuple[1]
        movie['title'] = movie_tuple[2]
        movie['title_sort'] = movie_tuple[3]
        movie['tagline'] = movie_tuple[4]
        movie['content_rating'] = movie_tuple[5]
        movie['user_fields'] = movie_tuple[6]
        compare = movie_tuple[6]

        if movie['user_fields'] != '':
            movie['user_fields'] = movie_tuple[6].split('=')[1].split('|')
        else:
            movie['user_fields'] = list()

        if ".themoviedb" in movie['guid']:
            movie['tmdb_id'] = movie['guid'].split('//')[1].split('?')[0]
            movie['imdb_id'] = None
        elif ".imdb" in movie['guid']:
            movie['tmdb_id'] = None
            movie['imdb_id'] = movie['guid'].split('//')[1].split('?')[0]
        else:
            print("Unable find either an imdb-ID or a tmdb-ID for " + movie['title'] + ", skipping movie.")
            continue

        movie['metadata_items_jobs'] = dict()

        process_movie(movie)

        process_collection(movie)

        if len(movie['user_fields']) > 0:
            movie['user_fields'].sort(key=int)
            movie['user_fields'] = 'lockedFields=' + '|'.join(movie['user_fields'])
            if movie['user_fields'] != compare:
                movie['metadata_items_jobs']['user_fields'] = movie['user_fields']
        metadata_items_commits[movie['metadata_id']] = movie['metadata_items_jobs']

        tmdb_movie_metadata = None
        secondary_tmdb_movie_metadata = None
        tmdb_collection_metadata = None
        secondary_tmdb_collection_metadata = None

    # Commit to database.
    commit_to_database()

    database.close()


def backup_database(source_dir, target_dir):

    copyfile(source_dir, target_dir)


def process_movie(movie):

    def change_sort_titles():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("2" == s for s in movie['user_fields']):
                    return

        if tmdb_movie_metadata is None:
            if movie['tmdb_id'] is None:
                get_tmdb_movie_id(movie)
            get_tmdb_movie_metadata(movie, main_language)
        if secondary_tmdb_movie_metadata is None:
            get_secondary_tmdb_movie_metadata(movie, secondary_language)

        if tmdb_movie_metadata['title'] == secondary_tmdb_movie_metadata['title']:
            title_sort = tmdb_movie_metadata['title']
        elif not settings.getboolean('invert_title_positions'):
            title_sort = tmdb_movie_metadata['title'] + ' ' \
                              + settings['title_sort_delimiter'] + ' ' \
                              + secondary_tmdb_movie_metadata['title']
        else:
            title_sort = secondary_tmdb_movie_metadata['title'] + ' ' \
                              + settings['title_sort_delimiter'] + ' ' \
                              + tmdb_movie_metadata['title']

        movie['metadata_items_jobs']['title_sort'] = title_sort

        if settings.getboolean('lock_after_completion') and '2' not in movie['user_fields']:
            movie['user_fields'].append('2')

    def change_content_ratings():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("8" == s for s in movie['user_fields']):
                    return
            if any(movie['content_rating'] == s for s in config['RATINGS']):
                return
            if movie['content_rating'] == settings['unknown_content_rating']:
                return

        if movie['imdb_id'] is None:
            get_tmdb_movie_metadata(movie, main_language)
        content_rating = get_imdb_content_rating(movie, settings['content_rating_country_code'])

        found = False
        for to_rating, from_rating in config.items('RATINGS'):
            if from_rating.lower() == content_rating.lower():
                content_rating = to_rating
                found = True
        if not found:
            content_rating = '???'

        movie['metadata_items_jobs']['content_rating'] = content_rating
        if settings.getboolean('lock_after_completion') and '8' not in movie['user_fields']:
            movie['user_fields'].append('8')

    def add_missing_tagline():
        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("7" == s for s in movie['user_fields']):
                    return
            if movie['tagline'] != '':
                return

        if tmdb_movie_metadata is None:
            if movie['tmdb_id'] is None:
                get_tmdb_movie_id(movie)
            get_tmdb_movie_metadata(movie, main_language)
        if secondary_tmdb_movie_metadata is None:
            get_secondary_tmdb_movie_metadata(movie, secondary_language)

        if settings.getboolean('prefer_secondary_language'):
            if secondary_tmdb_movie_metadata['tagline'] != '':
                tagline = secondary_tmdb_movie_metadata['tagline']
            else:
                tagline = tmdb_movie_metadata['tagline']
        else:
            if tmdb_movie_metadata['tagline'] != '':
                tagline = tmdb_movie_metadata['tagline']
            else:
                tagline = secondary_tmdb_movie_metadata['tagline']

        if not tagline == '':
            movie['metadata_items_jobs']['tagline'] = tagline
            if settings.getboolean('lock_after_completion') and '7' not in movie['user_fields']:
                movie['user_fields'].append('7')

    def convert_genres():
        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("15" == s for s in movie['user_fields']):
                    return

        tags_list = dict()
        for tags_info in cursor.execute("SELECT id, tag "
                                        "FROM tags "
                                        "WHERE tag_type = 1 "):
            tags_list[tags_info[1].lower()] = tags_info[0]
        movie['tags_list'] = tags_list

        taggings_list = dict()
        for tag_id in tags_list:
            cursor.execute("SELECT id, tag_id "
                           "FROM taggings "
                           "WHERE metadata_item_id = " + str(movie['metadata_id']) + " "
                           "AND tag_id = " + str(tags_list[tag_id]))
            tag = cursor.fetchone()
            if tag is None:
                continue
            taggings_list[tag[0]] = tag[1]
        movie['taggings_list'] = taggings_list

        for rename_to, rename_from_list in config.items('GENRES'):
            rename_from_list = rename_from_list.split(',')
            for rename_from in rename_from_list:
                for tagging_id in movie['taggings_list']:

                    if rename_from.lower() not in movie['tags_list']:
                        continue

                    elif rename_to.lower() not in movie['tags_list']:
                        if movie['tags_list'][rename_from.lower()] in tags_commits:
                            tags_commits[movie['tags_list'][rename_from.lower()]]['tag'] = rename_to.lower().capitalize()
                        else:
                            tags_commits[movie['tags_list'][rename_from.lower()]] = {'tag': rename_to.lower().capitalize()}

                    elif movie['taggings_list'][tagging_id] == movie['tags_list'][rename_from.lower()]:
                        if tagging_id in taggings_commits:
                            taggings_commits[tagging_id]['tag_id'] = movie['tags_list'][rename_to]
                        else:
                            taggings_commits[tagging_id] = {'tag_id': movie['tags_list'][rename_to]}

        if settings.getboolean('lock_after_completion') and '15' not in movie['user_fields']:
            movie['user_fields'].append('15')

    # change genres.
    settings = config['SORT_TITLE_SETTINGS']
    if settings.getboolean('change_sort_titles', False):
        change_sort_titles()

    # change movie content rating.
    settings = config['CONTENT_RATING_SETTINGS']
    if settings.getboolean('change_content_ratings', False):
        change_content_ratings()

    # add missing tagline.
    settings = config['TAGLINE_SETTINGS']
    if settings.getboolean('add_missing_tagline', False):
        add_missing_tagline()

    # convert genres.
    settings = config['GENRES_SETTINGS']
    if settings.getboolean('convert_genres', False):
        convert_genres()

    # add to collection.


def process_collection(movie):
    pass

    # add movie to collection.

    # change name of the collection.

    # change poster of the collection.

    # change artwork of the collection.

    # change sort title of the collection.

    # change content rating of the collection.

    # add description of the collection.


def commit_to_database():

    for metadata_id, d in metadata_items_commits.items():
        if len(d) == 0:
            continue
        command = 'UPDATE metadata_items SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
            print(str(column) + " : " + str(value))
        command = command[:-1]
        command += ' WHERE id = ' + str(metadata_id)
        main_cursor.execute(command)

    for tagging_id, d in taggings_commits.items():
        if len(d) == 0:
            continue
        command = 'UPDATE taggings SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
        command = command[:-1]
        command += ' WHERE id = ' + str(tagging_id)
        main_cursor.execute(command)

    for tag_id, d in tags_commits.items():
        if len(d) == 0:
            continue
        command = 'UPDATE tags SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
        command = command[:-1]
        command += ' WHERE id = ' + str(tag_id)
        main_cursor.execute(command)

    database.commit()


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


def get_tmdb_movie_id(movie):
    response = retrieve_web_page('https://api.themoviedb.org/3/find/'
                                 + movie['imdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=en-US'
                                 '&external_source=imdb_id', 'tmdb id')

    data = json.loads(response.read().decode('utf-8'))
    movie['tmdb_id'] = str(data['movie_results'][0]['id'])
    response.close()


def get_tmdb_movie_metadata(movie, tmdb_language_code):
    global tmdb_movie_metadata

    response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                                 + movie['tmdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + tmdb_language_code, 'english movie metadata from tmdb')

    tmdb_movie_metadata = json.loads(response.read().decode('utf-8'))
    movie['imdb_id'] = tmdb_movie_metadata['imdb_id']
    response.close()


def get_secondary_tmdb_movie_metadata(movie, tmdb_language_code):
    global secondary_tmdb_movie_metadata

    response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                                 + movie['tmdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + tmdb_language_code, 'foreign movie metadata from tmdb')

    secondary_tmdb_movie_metadata = json.loads(response.read().decode('utf-8'))
    response.close()


def get_imdb_content_rating(movie, country):
    response = retrieve_web_page("https://www.imdb.com/title/" + movie['imdb_id'] +
                                 "/parentalguide?ref_=tt_ql_stry_5", 'certification page on imdb')
    data = response.read()
    response.close()
    soup = BeautifulSoup(data)
    cert = str(soup.findAll("a", {"href": lambda l: l and l.startswith('/search/title?certificates=' + country + ':')}))
    try:
        cert = cert.split(':')[1].split('"')[0]
    except IndexError:
        print(movie['title'] + 'don\'t have a content rating on imdb for this country.')
        cert = ':-???-:'

    return cert


main()

sys.exit()
