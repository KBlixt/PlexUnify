import codecs
import sys
import configparser
import time
import json
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from shutil import copyfile
from bs4 import BeautifulSoup

# these packages need to be installed:
from plexapi.server import PlexServer
import sqlite3

# global static variables:

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
main_language = global_settings['main_tmdb_language']
secondary_language = global_settings['secondary_tmdb_language']

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
# end of global static variables.

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

    def get_movie_data(metadata_id):

        cursor.execute('SELECT id, guid, title, original_title, tagline, content_rating, user_fields '
                       'FROM metadata_items '
                       'WHERE id = ? ', (metadata_id,))
        movie_info = cursor.fetchone()
        movie_ret = dict()
        movie_ret['metadata_id'] = movie_info[0]
        movie_ret['guid'] = movie_info[1]
        movie_ret['title'] = movie_info[2]
        movie_ret['original_title'] = movie_info[3]
        movie_ret['tagline'] = movie_info[4]
        movie_ret['content_rating'] = movie_info[5]
        movie_ret['user_fields'] = movie_info[6]
        movie_ret['user_fields_compare'] = movie_info[6]

        if movie_ret['user_fields'] != '':
            movie_ret['user_fields'] = movie_info[6].split('=')[1].split('|')
        else:
            movie_ret['user_fields'] = list()

        if ".themoviedb" in movie_ret['guid']:
            movie_ret['tmdb_id'] = movie_ret['guid'].split('//')[1].split('?')[0]
            movie_ret['imdb_id'] = None
        elif ".imdb" in movie_ret['guid']:
            movie_ret['tmdb_id'] = None
            movie_ret['imdb_id'] = movie_ret['guid'].split('//')[1].split('?')[0]

        movie_ret['metadata_items_jobs'] = dict()

        return movie_ret

    def report_movie_to_commit():

        temp = list()
        for value in movie['user_fields']:
            if value != '':
                temp.append(value)
        movie['user_fields'] = temp

        if len(movie['user_fields']) > 0:
            movie['user_fields'].sort(key=int)
            movie['user_fields'] = 'lockedFields=' + '|'.join(movie['user_fields'])
            if movie['user_fields'] != movie['user_fields_compare']:
                movie['metadata_items_jobs']['user_fields'] = movie['user_fields']
        metadata_items_commits[movie['metadata_id']] = movie['metadata_items_jobs']

        tmdb_movie_metadata = None
        secondary_tmdb_movie_metadata = None

    def get_collection_data():
        settings = config['COLLECTIONS_SETTINGS']
        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("16" == s for s in movie['user_fields']):
                    return

        collection_ret = dict()
        collection_ret['name'] = None
        collection_ret['movies_in_collection'] = list()
        collection_ret['movies_in_collection'].append(movie)

        if settings.getboolean('prefer_secondary_language'):
            if secondary_tmdb_movie_metadata is None:
                get_secondary_tmdb_movie_metadata(movie)
            if secondary_tmdb_movie_metadata['belongs_to_collection'] is None:
                return None
            collection_ret['collection_id'] = secondary_tmdb_movie_metadata['belongs_to_collection']['id']
            collection_ret['title'] = secondary_tmdb_movie_metadata['belongs_to_collection']['name']
            get_secondary_tmdb_collection_metadata(collection_ret)
            current_metadata_holder = secondary_tmdb_collection_metadata

        else:
            if tmdb_movie_metadata is None:
                get_tmdb_movie_metadata(movie)
            if tmdb_movie_metadata['belongs_to_collection'] is None:
                return None
            collection_ret['collection_id'] = tmdb_movie_metadata['belongs_to_collection']['id']
            collection_ret['title'] = tmdb_movie_metadata['belongs_to_collection']['name']
            get_tmdb_collection_metadata(collection_ret)
            current_metadata_holder = tmdb_collection_metadata

        movies_above_score_threshold = 0
        total_score = 0
        for coll_movie in current_metadata_holder['parts']:
            if coll_movie['vote_average'] > settings.getfloat('minimum_movie_score'):
                movies_above_score_threshold += 1
            total_score += settings.getint('minimum_movie_score')

        stat1 = total_score < settings.getint('minimum_total_score')
        stat2 = movies_above_score_threshold < settings.getint('minimum_movie_count')
        if not (stat1 and stat2):
            if settings.getboolean('enable_automatic_deletion', False):
                delete_collection(collection_ret['title'])
            return None

        suffix_list = settings.get('collection_suffixes_to_remove')
        suffix_list = suffix_list.replace(' ', '')
        suffix_list = suffix_list.split(',')
        for suffix in suffix_list:
            if collection_ret['title'].lower().endswith(suffix.lower()):
                collection_ret['title'] = collection_ret['title'][:-(len(suffix) + 1)]
                break

        global database
        global cursor

        collection_info = None
        created_collection = False
        for i in range(100):
            time.sleep(1)
            cursor.execute('SELECT id, content_rating, user_fields, [index], hash '
                           'FROM metadata_items '
                           'WHERE metadata_type = 18 '
                           'AND library_section_id = ? '
                           'AND title = ? ', (library_key, collection_ret['title'],))
            collection_info = cursor.fetchone()
            if collection_info is None:
                if not created_collection:
                    if settings.getboolean('add_new_collections'):
                        library.get(movie['title']).addCollection(collection_ret['title'])
                        library.get(movie['title']).reload()
                        created_collection = True

                        database.commit()
                        # time.sleep(1)
                        # database.close()
                        # time.sleep(1)
                        # database = sqlite3.connect(database_dir)
                        # cursor = database.cursor()

                    else:
                        return None
                else:
                    print('waiting.')
                    time.sleep(1)
            else:
                break

        collection_ret['metadata_id'] = collection_info[0]
        collection_ret['content_rating'] = collection_info[1]
        collection_ret['user_fields'] = collection_info[2]
        collection_ret['index'] = collection_info[3]
        collection_ret['hash'] = collection_info[4]

        if collection_ret['user_fields'] != '':
            collection_ret['user_fields'] = collection_info[2].split('=')[1].split('|')
        else:
            collection_ret['user_fields'] = list()

        cursor.execute('SELECT taggings.metadata_item_id '
                       'FROM tags '
                       'INNER JOIN taggings '
                       'ON tags.tag_type = 2 '
                       'AND tags.id = taggings.tag_id '
                       'AND tags.id = ?', (collection_ret['index'],))
        for movie_id in cursor.fetchall():
            collection_ret['movies_in_collection'].append(get_movie_data(movie_id[0]))

        if '29' not in collection_ret['user_fields']:
            return collection_ret
        else:
            return None

    def report_collection_to_commit():
        pass

    global tmdb_movie_metadata
    global secondary_tmdb_movie_metadata
    global tmdb_collection_metadata
    global secondary_tmdb_collection_metadata

    # Backup Database.
    backup_database(database_dir, database_backup_dir)

    cursor.execute('SELECT id '
                   'FROM metadata_items '
                   'WHERE library_section_id = ? '
                   'AND metadata_type = 1 '
                   'ORDER BY title ASC '
                   'LIMIT ?', (library_key, str(global_settings.getint('modify_limit', 30)),))

    for current_movie_id in cursor.fetchall():

        movie = get_movie_data(current_movie_id[0])

        process_movie(movie)

        report_movie_to_commit()

        collection = get_collection_data()

        if config.getboolean('COLLECTIONS_SETTINGS', 'add_movies_to_collections') and collection is not None:
            process_collection(collection)

            report_collection_to_commit()

        tmdb_movie_metadata = None
        secondary_tmdb_movie_metadata = None
        tmdb_collection_metadata = None
        secondary_tmdb_collection_metadata = None

    # Commit to database.
    commit_to_database()

    return


def backup_database(source_dir, target_dir):

    copyfile(source_dir, target_dir)


def process_movie(movie):

    def check_main_language_metadata():
        if tmdb_movie_metadata is None:
            get_tmdb_movie_metadata(movie)

    def check_secondary_language_metadata():
        if secondary_tmdb_movie_metadata is None:
            get_tmdb_movie_metadata(movie)

    def change_original_titles():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("3" == s for s in movie['user_fields']):
                    return

        check_main_language_metadata()
        check_secondary_language_metadata()

        if tmdb_movie_metadata['title'] == secondary_tmdb_movie_metadata['title']:
            original_title = tmdb_movie_metadata['title']
        elif not settings.getboolean('prefer_secondary_language'):
            original_title = tmdb_movie_metadata['title'] + ' ' \
                              + settings['title_delimiter'] + ' ' \
                              + secondary_tmdb_movie_metadata['title']
        else:
            original_title = secondary_tmdb_movie_metadata['title'] + ' ' \
                              + settings['title_delimiter'] + ' ' \
                              + tmdb_movie_metadata['title']

        movie['metadata_items_jobs']['original_title'] = original_title
        if settings.getboolean('lock_after_completion') and '3' not in movie['user_fields']:
            movie['user_fields'].append('3')

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
            get_tmdb_movie_metadata(movie)
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

        if settings.getboolean('prefer_secondary_language'):
            check_secondary_language_metadata()
            if secondary_tmdb_movie_metadata['tagline'] != '':
                tagline = secondary_tmdb_movie_metadata['tagline']
            else:
                check_main_language_metadata()
                tagline = tmdb_movie_metadata['tagline']
        else:
            check_main_language_metadata()
            if tmdb_movie_metadata['tagline'] != '':
                tagline = tmdb_movie_metadata['tagline']
            else:
                check_secondary_language_metadata()
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
                        add_to_commit_list(tags_commits,
                                           movie['tags_list'][rename_from.lower()],
                                           'tag',
                                           rename_to.title())

                    elif movie['taggings_list'][tagging_id] == movie['tags_list'][rename_from.lower()]:
                        add_to_commit_list(taggings_commits,
                                           tagging_id,
                                           'tag_id',
                                           movie['tags_list'][rename_to.lower()])

        if settings.getboolean('lock_after_completion') and '15' not in movie['user_fields']:
            movie['user_fields'].append('15')

    print('Processing : "' + movie['title'] + '"')

    # change genres.
    try:
        settings = config['ORIGINAL_TITLE_SETTINGS']
        if settings.getboolean('change_original_title', False):
            change_original_titles()
    except ValueError as e:
        print(e)

    # change movie content rating.
    try:
        settings = config['CONTENT_RATING_SETTINGS']
        if settings.getboolean('change_content_ratings', False):
            change_content_ratings()
    except ValueError as e:
        print(e)

    # add missing tagline.
    try:
        settings = config['TAGLINE_SETTINGS']
        if settings.getboolean('add_missing_tagline', False):
            add_missing_tagline()
    except ValueError as e:
        print(e)

    # convert genres.
    settings = config['GENRES_SETTINGS']
    if settings.getboolean('convert_genres', False):
        convert_genres()


def process_collection(collection):
    pass

    # Add other movies into the collection.

    # Calculate content rating.

    # Add overview.

    # Add Poster.

    # Add background art.


def commit_to_database():

    if global_settings.getboolean('prompt_before_committing', True):
        print('-----------------------------------------------------------')
        print('The script is now ready to write to your database.')
        print('Please turn off Plex media server until the script is done.')
        print('The write process is fairly quick.')
        print('-----------------------------------------------------------')
        cont = input("Do you wish to proceed? yes/no > ")
        while cont.lower() not in ("yes", "no"):
            cont = input("Do you wish to proceed? yes/no > ")
        if cont == "no":
            print('Exiting.')
            database.close()
            sys.exit()
        print('Committing...')

    for metadata_id, d in metadata_items_commits.items():
        if len(d) == 0:
            continue
        command = 'UPDATE metadata_items SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
        command = command[:-1]
        command += ' WHERE id = ' + str(metadata_id)
        cursor.execute(command)

    for tagging_id, d in taggings_commits.items():
        if len(d) == 0:
            continue
        command = 'UPDATE taggings SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
        command = command[:-1]
        command += ' WHERE id = ' + str(tagging_id)
        cursor.execute(command)

    for tag_id, d in tags_commits.items():
        if len(d) == 0:
            continue
        command = 'UPDATE tags SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
        command = command[:-1]
        command += ' WHERE id = ' + str(tag_id)
        cursor.execute(command)

    database.commit()
    database.close()
    print('-----------------------------------------------------------')
    print('The writing process is now over.')
    print('you may turn on your Plex server now.')


def retrieve_web_page(url, page_name='page'):

    response = None

    for attempt in range(20):
        try:
            response = urlopen(url)
            break
        except HTTPError as e:
            raise ValueError('Failed to download ' + page_name + ' : ' + e.msg)
        except URLError:
            print('Failed to download ' + page_name + '. Trying again in 10 seconds')
            time.sleep(10)
            if attempt > 8:
                print('You might have lost internet connection.')
                print('Breaking out of loop and committing')
                commit_to_database()
                sys.exit()

    return response


def get_tmdb_movie_id(movie):
    response = retrieve_web_page('https://api.themoviedb.org/3/find/'
                                 + movie['imdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=en-US'
                                 '&external_source=imdb_id', 'tmdb id')

    data = json.loads(response.read().decode('utf-8'))

    if len(data['movie_results']) == 0:
        raise ValueError('Unable to find TMDB ID. Skipping.')

    movie['tmdb_id'] = str(data['movie_results'][0]['id'])
    response.close()


def get_tmdb_movie_metadata(movie):
    global tmdb_movie_metadata

    if movie['tmdb_id'] is None:
        get_tmdb_movie_id(movie)

    response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                                 + movie['tmdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + main_language, 'Main language movie metadata from tmdb')

    tmdb_movie_metadata = json.loads(response.read().decode('utf-8'))
    if len(tmdb_movie_metadata['imdb_id']) != 9:
        raise ValueError("Unable to find IMDB ID. Skipping.")
    movie['imdb_id'] = tmdb_movie_metadata['imdb_id']
    response.close()


def get_secondary_tmdb_movie_metadata(movie):
    global secondary_tmdb_movie_metadata

    response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                                 + movie['tmdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + secondary_language, 'Secondary language movie metadata from tmdb')

    secondary_tmdb_movie_metadata = json.loads(response.read().decode('utf-8'))
    response.close()


def get_tmdb_collection_metadata(collection):
    global tmdb_collection_metadata

    response = retrieve_web_page('https://api.themoviedb.org/3/collection/' + str(collection['collection_id']) +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + main_language, 'Main language collection metadata from tmdb')

    tmdb_collection_metadata = json.loads(response.read().decode('utf-8'))

    response.close()


def get_secondary_tmdb_collection_metadata(collection):
    global secondary_tmdb_collection_metadata

    response = retrieve_web_page('https://api.themoviedb.org/3/collection/' + str(collection['collection_id']) +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + secondary_language, 'Secondary language collection metadata from tmdb')

    secondary_tmdb_collection_metadata = json.loads(response.read().decode('utf-8'))

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
        print('The movie "' + movie['title'] + '" don\'t have a content rating on imdb for this country.')
        cert = ':-???-:'

    return cert


def add_to_commit_list(commit_list, entry_id, key, value):

    if entry_id in commit_list:
        commit_list[entry_id][key] = value
    else:
        commit_list[entry_id] = {key: value}

def delete_collection(title_name):
    pass
main()

sys.exit()
