import codecs
import sys
import configparser
import time
import json
import os
import errno
from socket import timeout
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from requests.exceptions import ConnectionError
from shutil import copyfile
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

# these packages need to be installed:
# pip install plexapi
# pip install sqlite3

try:
    # noinspection PyUnresolvedReferences
    from plexapi.server import PlexServer
    # noinspection PyUnresolvedReferences
    from plexapi.exceptions import NotFound
    plex_api_installed = True
except ImportError:
    plex_api_installed = False
    print('Plex api is not installed.')


# global static variables:
# config stuff
config_file = 'config.cfg'
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

# database stuff
plex_home_dir = global_settings.get('plex_home_directory')
database_dir = os.path.join(plex_home_dir, 'Plug-in Support', 'Databases', 'com.plexapp.plugins.library.db')

database_backup_dir = global_settings.get('database_backup_dir', os.path.join(plex_home_dir,
                                                                              'Plug-in Support',
                                                                              'Databases.PlexUnify.Backups',
                                                                              'Database-Backup'))
database = sqlite3.connect(database_dir)
cursor = database.cursor()
main_cursor = database.cursor()

# plex api stuff
library_key = None
if plex_api_installed:
    plex_server_ip_address = global_settings['plex_server_ip_address']
    plex_auth_token = global_settings['plex_auth_token']
    try:
        plex_server = PlexServer(plex_server_ip_address, plex_auth_token)
        try:
            library = plex_server.library.section(global_settings['library_to_modify'])
            library_key = library.key
        except NotFound:
            library_key = None
            plex_api_installed = False

    except ConnectionError as conne:
        print(conne.args)
        print('Unable to connect to your Plex server.')
        plex_api_installed = False


if library_key is None:
    cursor.execute('SELECT id '
                   'FROM library_sections '
                   'WHERE section_type = 1 '
                   'AND name = ?', (global_settings['library_to_modify'],))
    fetch = cursor.fetchone()
    if fetch is not None:
        library_key = fetch[0]
    else:
        print('No library on Plex called "' + global_settings['library_to_modify'] + '"')
        database.close()
        sys.exit()
# end of global static variables.

# global variables:

tmdb_movie_metadata = None
secondary_tmdb_movie_metadata = None
tmdb_collection_metadata = None
secondary_tmdb_collection_metadata = None

metadata_items_commits = dict()   #
taggings_commits = dict()         #
tags_commits = dict()             # list of changes to be made on tables. example: 20: {dict of changes}


delete_commits = list()           # list of entries to be deleted. example: [20, 3201]

taggings_insert_commits = dict()  # list of added entries to tables. example: 20: {dict of added entries}
# end of global variables.


def main():

    def get_movie_data(metadata_id):

        cursor.execute('SELECT id, guid, title, original_title, tagline, content_rating, user_fields, hash '
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
        movie_ret['hash'] = movie_info[7]

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

        if movie_ret['metadata_id'] in metadata_items_commits:
            if 'inherited_data' in metadata_items_commits[movie_ret['metadata_id']]:
                if 'user_fields' in metadata_items_commits[movie_ret['metadata_id']]['inherited_data']:
                    movie_ret['user_fields'] += \
                        metadata_items_commits[movie_ret['metadata_id']]['inherited_data']['user_fields']

        return movie_ret

    def report_movie_to_commit():

        temp = list()
        for value in movie['user_fields']:
            if value != '':
                temp.append(value)
        movie['user_fields'] = temp

        if len(movie['user_fields']) > 0:
            movie['user_fields'] = list(set(movie['user_fields']))
            movie['user_fields'].sort(key=int)
            movie['user_fields'] = 'lockedFields=' + '|'.join(movie['user_fields'])
            if movie['user_fields'] != movie['user_fields_compare']:
                movie['metadata_items_jobs']['user_fields'] = movie['user_fields']

        if len(movie['metadata_items_jobs']) != 0:
            metadata_items_commits[movie['metadata_id']] = movie['metadata_items_jobs']

    def get_collection_data():

        #     cursor.execute('SELECT tags.name, taggings.metadata_item_id '
        #                    'FROM tags '
        #                    'INNER JOIN taggings '
        #                    'ON taggings.tag_id = tags.id '
        #                    'AND tags.tag_type = 2 '
        #                    'WHERE taggings.metadata_item_id = ?', (movie['metadata_id'],))

        def get_metadata_holder():

            global secondary_tmdb_collection_metadata
            global tmdb_collection_metadata

            if settings.getboolean('prefer_secondary_language'):
                if secondary_tmdb_movie_metadata is None:
                    get_secondary_tmdb_movie_metadata(movie)
                movie_metadata = secondary_tmdb_movie_metadata
            else:
                if tmdb_movie_metadata is None:
                    get_tmdb_movie_metadata(movie)
                movie_metadata = tmdb_movie_metadata

            if movie_metadata['belongs_to_collection'] is None:
                return None

            collection_ret['collection_id'] = movie_metadata['belongs_to_collection']['id']
            collection_ret['title'] = movie_metadata['belongs_to_collection']['name']

            if settings.getboolean('prefer_secondary_language'):
                get_secondary_tmdb_collection_metadata(collection_ret)
                coll_metadata = secondary_tmdb_collection_metadata
            else:
                get_tmdb_collection_metadata(collection_ret)
                coll_metadata = tmdb_collection_metadata

            return coll_metadata

        def trim_suffix(title):
            suffix_list = settings.get('collection_suffixes_to_remove')
            suffix_list = suffix_list.replace(' ', '')
            suffix_list = suffix_list.split(',')
            for suffix in suffix_list:
                if title.lower().endswith(suffix.lower()):
                    title = title[:-(len(suffix) + 1)]
                    break
            return title

        def is_viable():

            movies_above_score_threshold = 0
            total_score = 0
            for coll_movie in current_collection_metadata_holder['parts']:
                if coll_movie['vote_count'] > settings.getint('minimum_movie_vote_count'):
                    if coll_movie['vote_average'] >= settings.getfloat('minimum_movie_score', 0):
                        movies_above_score_threshold += 1
                total_score += coll_movie['vote_average']

            stat1 = total_score >= settings.getint('minimum_total_score')
            stat2 = movies_above_score_threshold >= settings.getint('minimum_movie_count')
            if not (stat1 and stat2):
                if settings.getboolean('enable_automatic_deletion', False):
                    delete_collection(collection_ret, settings.get('delete_locked_less_than'))
                return False

            return True

        def get_collection_info():
            global library
            coll_info = None
            created_collection = False
            for i in range(5):

                cursor.execute('SELECT id, content_rating, user_fields, [index], hash, summary '
                               'FROM metadata_items '
                               'WHERE metadata_type = 18 '
                               'AND library_section_id = ? '
                               'AND title = ? ', (library_key, collection_ret['title'],))
                coll_info = cursor.fetchone()
                if coll_info is None:
                    if not created_collection:
                        created_collection = True
                        if settings.getboolean('add_new_collections') or settings.getboolean('force'):
                            if plex_api_installed:
                                library.get(movie['title']).addCollection(collection_ret['title'])
                                database.commit()
                                movie['user_fields_compare'] = 'force_push'
                            else:
                                print('Unable to create new collection because Plex api is unavailable. Skipping')
                                return None

                        else:
                            print('Not allowed to create new collections. Skipping')
                            return None
                    else:
                        print('Waiting for database to add collection: "' + collection_ret['title'] + '"')
                        time.sleep(1)
                        database.commit()
                else:
                    break
            if coll_info is None:
                print('was unable to find collection: "' + collection_ret['title'] + '". Skipping')
                return None

            return coll_info

        collection_ret = dict()
        collection_ret['name'] = None
        collection_ret['movies_in_collection'] = list()
        collection_ret['movies_in_collection'].append(movie)

        current_collection_metadata_holder = get_metadata_holder()

        if current_collection_metadata_holder is None:
            return None

        collection_ret['title'] = trim_suffix(collection_ret['title'])

        if not is_viable():
            return None

        collection_info = get_collection_info()
        if collection_info is None:
            return None

        collection_ret['metadata_id'] = collection_info[0]
        collection_ret['content_rating'] = collection_info[1]
        collection_ret['user_fields'] = collection_info[2]
        collection_ret['user_fields_compare'] = collection_info[2]
        collection_ret['index'] = collection_info[3]
        collection_ret['hash'] = collection_info[4]
        collection_ret['summary'] = collection_info[5]

        if collection_ret['user_fields'] != '':
            collection_ret['user_fields'] = collection_info[2].split('=')[1].split('|')
        else:
            collection_ret['user_fields'] = list()

        collection_ret['metadata_items_jobs'] = dict()

        cursor.execute('SELECT taggings.metadata_item_id '
                       'FROM tags '
                       'INNER JOIN taggings '
                       'ON tags.tag_type = 2 '
                       'AND tags.id = taggings.tag_id '
                       'AND tags.id = ?', (collection_ret['index'],))
        for movie_id in cursor.fetchall():
            if movie_id[0] != movie['metadata_id']:
                collection_ret['movies_in_collection'].append(get_movie_data(movie_id[0]))

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if '16' in movie['user_fields']:
                    return collection_ret

        cursor.execute('SELECT id '
                       'FROM taggings '
                       'WHERE metadata_item_id = ? '
                       'AND tag_id = ?', (movie['metadata_id'], collection_ret['index'],))
        if cursor.fetchone() is not None:
            return collection_ret

        add_to_insert_commit_list(taggings_insert_commits,
                                  movie['metadata_id'],
                                  'metadata_item_id',
                                  movie['metadata_id'])
        add_to_insert_commit_list(taggings_insert_commits,
                                  movie['metadata_id'],
                                  'tag_id',
                                  collection_ret['index'])
        add_to_insert_commit_list(taggings_insert_commits,
                                  movie['metadata_id'],
                                  '[index]',
                                  '10')


        return collection_ret

    def report_collection_to_commit():

        temp = list()
        for value in collection['user_fields']:
            if value != '':
                temp.append(value)
                collection['user_fields'] = temp

        if len(collection['user_fields']) > 0:
            collection['user_fields'] = list(set(collection['user_fields']))
            collection['user_fields'].sort(key=int)
            collection['user_fields'] = 'lockedFields=' + '|'.join(collection['user_fields'])
            if collection['user_fields'] != collection['user_fields_compare']:
                collection['metadata_items_jobs']['user_fields'] = collection['user_fields']

        if len(collection['metadata_items_jobs']) != 0:
            metadata_items_commits[collection['metadata_id']] = collection['metadata_items_jobs']

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
                   'ORDER BY created_at DESC ' 
                   'LIMIT ?', (library_key, str(global_settings.getint('modify_limit', 30)),))

    for current_movie_id in cursor.fetchall():

        movie = get_movie_data(current_movie_id[0])

        process_movie(movie)

        settings = config['COLLECTIONS_SETTINGS']
        if settings.getboolean('enable_category'):
            try:
                collection = get_collection_data()
            except ValueError as e:
                print(e)
            else:
                if collection is not None:
                    process_collection(collection)

                    report_collection_to_commit()

        report_movie_to_commit()

        tmdb_movie_metadata = None
        secondary_tmdb_movie_metadata = None
        tmdb_collection_metadata = None
        secondary_tmdb_collection_metadata = None

    # Commit to database.
    commit_to_database()

    return


def backup_database(source_dir, target_dir):

    if not os.path.isdir(os.path.split(target_dir)[0]):
        os.makedirs(os.path.split(target_dir)[0], mode=0o777, exist_ok=True)

    if os.path.isfile(target_dir):
        for i in range(global_settings.getint('backups_to_keep', 5) - 1):
            if os.path.isfile(target_dir + str(-i)):
                copyfile(source_dir, target_dir + str(-(i + 1)))
    copyfile(source_dir, target_dir)


def process_movie(movie):

    def check_main_language_metadata():
        if tmdb_movie_metadata is None:
            get_tmdb_movie_metadata(movie)

    def check_secondary_language_metadata():
        if secondary_tmdb_movie_metadata is None:
            get_secondary_tmdb_movie_metadata(movie)

    def change_original_titles():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("3" == s for s in movie['user_fields']):
                    return

        if settings.getboolean('prefer_secondary_language'):
            check_secondary_language_metadata()
            original_title = secondary_tmdb_movie_metadata['original_title']
            added_title = secondary_tmdb_movie_metadata['title']
        else:
            check_main_language_metadata()
            original_title = tmdb_movie_metadata['original_title']
            added_title = tmdb_movie_metadata['title']

        if original_title in added_title or added_title in original_title:
            new_original_title = None
        else:
            new_original_title = original_title + ' ' + settings['title_delimiter'] + ' ' + added_title

        if new_original_title is not None:
            movie['metadata_items_jobs']['original_title'] = new_original_title

        if settings.getboolean('lock_after_completion') and '3' not in movie['user_fields']:
            movie['user_fields'].append('3')

    def change_content_ratings():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if any("8" == s for s in movie['user_fields']):
                    return
            if any(movie['content_rating'] == s for s in config['RATINGS']):
                return
            if movie['content_rating'].lower() == settings['unknown_content_rating'].lower():
                return

        if movie['imdb_id'] is None:
            get_tmdb_movie_metadata(movie)
            if movie['imdb_id'] is None:
                if settings.getboolean('lock_after_completion') and '8' not in movie['user_fields']:
                    movie['user_fields'].append('8')
                return

        content_rating = get_imdb_content_rating(movie, settings['content_rating_country_code'])

        found = False
        for to_rating, rename_from_list in config.items('RATINGS'):

            while ', ' in rename_from_list:
                rename_from_list = rename_from_list.replace(', ', ',')
            while ' ,' in rename_from_list:
                rename_from_list = rename_from_list.replace(' ,', ',')

            rename_from_list = rename_from_list.split(',')

            for from_rating in rename_from_list:
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
                if any("6" == s for s in movie['user_fields']):
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
                movie['user_fields'].append('6')

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
            while ', ' in rename_from_list:
                rename_from_list = rename_from_list.replace(', ', ',')
            while ' ,' in rename_from_list:
                rename_from_list = rename_from_list.replace(' ,', ',')

            rename_from_list = rename_from_list.split(',')
            new_tag_id = None
            for rename_from in rename_from_list:
                for tagging_id in movie['taggings_list']:

                    if rename_from.lower() == rename_to.lower():
                        continue

                    if rename_from.lower() not in movie['tags_list']:
                        continue

                    elif (rename_to.lower() not in movie['tags_list']) and (new_tag_id is None):

                        add_to_commit_list(tags_commits,
                                           movie['tags_list'][rename_from.lower()],
                                           'tag',
                                           rename_to.title())
                        new_tag_id = movie['tags_list'][rename_from]

                    elif (rename_to.lower() not in movie['tags_list']) and (new_tag_id is not None):
                        add_to_commit_list(taggings_commits,
                                           tagging_id,
                                           'tag_id',
                                           new_tag_id)

                    elif movie['taggings_list'][tagging_id] == movie['tags_list'][rename_from.lower()]:
                        add_to_commit_list(taggings_commits,
                                           tagging_id,
                                           'tag_id',
                                           movie['tags_list'][rename_to.lower()])

        if settings.getboolean('lock_after_completion') and '15' not in movie['user_fields']:
            movie['user_fields'].append('15')

    print('Processing movie: "' + movie['title'] + '"')

    # change genres.
    try:
        settings = config['ORIGINAL_TITLE_SETTINGS']
        if settings.getboolean('enable_category', False):
            change_original_titles()
    except ValueError as e:
        print(e)

    # change movie content rating.
    try:
        settings = config['CONTENT_RATING_SETTINGS']
        if settings.getboolean('enable_category', False):
            change_content_ratings()
    except ValueError as e:
        print(e)

    # add missing tagline.
    try:
        settings = config['TAGLINE_SETTINGS']
        if settings.getboolean('enable_category', False):
            add_missing_tagline()
    except ValueError as e:
        print(e)

    # convert genres.
    settings = config['GENRES_SETTINGS']
    if settings.getboolean('enable_category', False):
        convert_genres()


def process_collection(collection):

    def mass_symlink_creation(source_folder, target_folder, id_tag):
        if not os.path.isdir(target_folder):
            os.makedirs(target_folder, mode=0o777, exist_ok=True)
        for file in os.listdir(source_folder):
            source_file = os.path.join(source_folder, file)
            if len(file) > 35:
                target_file = os.path.join(target_folder, id_tag + file[-35:])
            else:
                target_file = os.path.join(target_folder, id_tag + file)
            if not os.path.exists(target_file):
                try:
                    os.symlink(source_file, target_file)
                except OSError as broken_symlink:
                    if broken_symlink.errno == errno.EEXIST:
                        os.remove(target_file)
                        os.symlink(source_file, target_file)

    def download_image(image_source, image_type, source_name):

        target = os.path.join(plex_home_dir, 'Metadata', 'Collections', collection['hash'][0],
                              collection['hash'][1:] + '.bundle', 'Uploads', image_type,
                              'g' + image_source[1:])

        picture_url = 'https://image.tmdb.org/t/p/original' + image_source

        download_dir = os.path.split(target)[0]

        if (not os.path.isfile(target)) or settings.getboolean('force'):

            if not os.path.isdir(download_dir):
                os.makedirs(download_dir, mode=0o777, exist_ok=True)

            with open(target, 'wb') as download_file:

                response = retrieve_web_page(picture_url, source_name)

                download_file.write(response.read())

    def check_main_language_metadata():
        if tmdb_collection_metadata is None:
            get_tmdb_collection_metadata(collection)

    def check_secondary_language_metadata():
        if secondary_tmdb_collection_metadata is None:
            get_secondary_tmdb_collection_metadata(collection)

    def update_content_rating():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if '8' in collection['user_fields']:
                    return

        found = False
        for content_rating in config.items('RATINGS'):
            if content_rating == collection['content_rating']:

                break

            for movie in collection['movies_in_collection']:
                if content_rating == movie['content_rating']:
                    collection['content_rating'] = movie['content_rating']
                    found = True
                    break
            if found:
                break

        if found:
            collection['metadata_items_jobs']['content_rating'] = collection['content_rating']
            if settings.getboolean('lock_after_completion') and '8' not in collection['user_fields']:
                collection['user_fields'].append('8')

    def add_overview():

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if '7' in collection['user_fields']:
                    return
                if collection['summary'] != '':
                    return
        found = False
        if settings.getboolean('prefer_secondary_language'):
            check_secondary_language_metadata()
            if secondary_tmdb_collection_metadata['overview'] != '':
                collection['metadata_items_jobs']['summary'] = secondary_tmdb_collection_metadata['overview']
                found = True
            else:
                check_main_language_metadata()
                if tmdb_collection_metadata['overview'] != '':
                    collection['metadata_items_jobs']['summary'] = tmdb_collection_metadata['overview']
                    found = True
        else:
            check_main_language_metadata()
            if tmdb_collection_metadata['overview'] != '':
                collection['metadata_items_jobs']['summary'] = tmdb_collection_metadata['overview']
                found = True
            else:
                check_secondary_language_metadata()
                if secondary_tmdb_collection_metadata['overview'] != '':
                    collection['metadata_items_jobs']['summary'] = secondary_tmdb_collection_metadata['overview']
                    found = True

        if found:
            if settings.getboolean('lock_after_completion') and '7' not in collection['user_fields']:
                collection['user_fields'].append('7')

    def add_poster():

        if settings.getboolean('symlink_movie_posters'):
            for movie in collection['movies_in_collection']:
                symlink_from = os.path.join(plex_home_dir, 'Metadata', 'Movies', movie['hash'][0],
                                            movie['hash'][1:] + '.bundle', 'Contents', '_combined', 'posters')
                symlink_to = os.path.join(plex_home_dir, 'Metadata', 'Collections', collection['hash'][0],
                                          collection['hash'][1:] + '.bundle', 'Uploads', 'posters')

                mass_symlink_creation(symlink_from, symlink_to, 'g' + movie['hash'][:5])

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if '9' in collection['user_fields']:
                    return

        if tmdb_collection_metadata is not None:
            current_metadata_holder = tmdb_collection_metadata
        elif secondary_tmdb_collection_metadata is not None:
            current_metadata_holder = secondary_tmdb_collection_metadata
        else:
            check_main_language_metadata()
            current_metadata_holder = tmdb_collection_metadata

        if current_metadata_holder['poster_path'] is not None:
            download_image(current_metadata_holder['poster_path'], 'posters', 'poster for collection')

            collection['metadata_items_jobs']['user_thumb_url'] = 'upload://posters/g' \
                                                                  + current_metadata_holder['poster_path'][1:]

        if settings.getboolean('add_movies_posters'):
            for movie in current_metadata_holder['parts']:
                if movie['poster_path'] is not None:
                    download_image(movie['poster_path'], 'poster', 'poster for collection')

        if settings.getboolean('lock_after_completion') and '9' not in collection['user_fields']:
            collection['user_fields'].append('9')

    def add_art():

        if settings.getboolean('symlink_movie_art'):
            for movie in collection['movies_in_collection']:
                symlink_from = os.path.join(plex_home_dir, 'Metadata', 'Movies', movie['hash'][0],
                                            movie['hash'][1:] + '.bundle', 'Contents', '_combined', 'art')
                symlink_to = os.path.join(plex_home_dir, 'Metadata', 'Collections', collection['hash'][0],
                                          collection['hash'][1:] + '.bundle', 'Uploads', 'art')

                mass_symlink_creation(symlink_from, symlink_to, 'g' + movie['hash'][:5])

        if not settings.getboolean('force'):
            if settings.getboolean('respect_lock'):
                if '10' in collection['user_fields']:
                    return

        if tmdb_collection_metadata is not None:
            current_metadata_holder = tmdb_collection_metadata
        elif secondary_tmdb_collection_metadata is not None:
            current_metadata_holder = secondary_tmdb_collection_metadata
        else:
            check_main_language_metadata()
            current_metadata_holder = tmdb_collection_metadata

        if current_metadata_holder['backdrop_path'] is not None:
            download_image(current_metadata_holder['backdrop_path'], 'art', 'art for collection')

            collection['metadata_items_jobs']['user_art_url'] = 'upload://art/g' \
                                                                + current_metadata_holder['backdrop_path'][1:]

        if settings.getboolean('add_movies_art'):
            for movie in current_metadata_holder['parts']:
                if movie['backdrop_path'] is not None:
                    download_image(current_metadata_holder['backdrop_path'], 'art', 'art from movies')

        if settings.getboolean('lock_after_completion') and '10' not in collection['user_fields']:
            collection['user_fields'].append('10')

    print('Processing collection: "' + collection['title'] + '"')

    # Calculate content rating.
    settings = config['COLLECTION_CONTENT_RATING_SETTINGS']
    try:
        if settings.getboolean('enable_category'):
            update_content_rating()
    except ValueError as e:
        print(e)

    # Add overview.
    settings = config['COLLECTIONS_SUMMARY_SETTINGS']
    try:
        if settings.getboolean('enable_category'):
            add_overview()
    except ValueError as e:
        print(e)

    # Add Poster.
    settings = config['COLLECTIONS_POSTER_SETTINGS']
    try:
        if settings.getboolean('enable_category'):
            add_poster()
    except ValueError as e:
        print(e)

    # Add background art.
    settings = config['COLLECTION_ARTWORK_SETTINGS']
    try:
        if settings.getboolean('enable_category'):
            add_art()
    except ValueError as e:
        print(e)


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
            return
        if plex_api_installed:
            try:
                PlexServer(plex_server_ip_address, plex_auth_token)
                print('-----------------------------------------------------------')
                print('I can still reach the server through Plex api.')
                print('-----------------------------------------------------------')
                cont = input("Do you still wish to proceed? yes/no > ")
                while cont.lower() not in ("yes", "no"):
                    cont = input("Do you still wish to proceed? yes/no > ")
                if cont == "no":
                    print('Exiting.')
                    database.close()
                    return

            except ConnectionError:
                pass

    print('Committing...')

    timestamp = datetime.now().replace(microsecond=0).isoformat(' ')
    for item in metadata_items_commits:
        if 'inherited_data' in metadata_items_commits[item]:
            metadata_items_commits[item].pop('inherited_data')
        metadata_items_commits[item]['refreshed_at'] = timestamp
        metadata_items_commits[item]['updated_at'] = timestamp

    for item in tags_commits:
        tags_commits[item]['updated_at'] = timestamp

    for item in taggings_insert_commits:
        taggings_insert_commits[item]['created_at'] = timestamp

    for metadata_id, d in metadata_items_commits.items():
        if len(d) <= 2:
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
        if len(d) <= 1:
            continue
        command = 'UPDATE tags SET'
        for column, value in d.items():
            command += ' ' + str(column) + ' = "' + str(value).replace('"', '') + '",'
        command = command[:-1]
        command += ' WHERE id = ' + str(tag_id)
        cursor.execute(command)

    for tag_id, d in taggings_insert_commits.items():
        if len(d) <= 1:
            continue
        command = 'INSERT INTO taggings '
        command_column_names = '('
        command_values = '('
        for column, value in d.items():
            command_column_names += column + ', '
            command_values += '"' + str(value) + '", '

        command_column_names = command_column_names[:-2] + ') '
        command_values = command_values[:-2] + ') '
        command += command_column_names + 'VALUES ' + command_values
        cursor.execute(command)

    for item in delete_commits:
        cursor.execute('UPDATE metadata_items SET metadata_type = "10000" WHERE id = ?', (item[0],))
        cursor.execute('DELETE FROM tags WHERE id = ?', (item[1],))
        cursor.execute('DELETE FROM taggings WHERE tag_id = ? AND metadata_item_id = ?', (item[1], item[0],))

    database.commit()
    database.close()
    print('-----------------------------------------------------------')
    print('The writing process is now over.')
    print('you may turn on your Plex server now.')


def retrieve_web_page(url, page_name='page'):

    response = None
    print('Downloading ' + page_name + '.')
    for attempt in range(20):
        try:
            response = urlopen(url, timeout=2)
            break
        except timeout:
            print('Failed to download ' + page_name + ' : timed out. Trying again in 2 seconds.')
            time.sleep(2)
            if attempt > 8:
                print('You might have lost internet connection.')
                print('Breaking out of loop and committing')
                commit_to_database()
                sys.exit()
        except HTTPError as e:
            raise ValueError('Failed to download ' + page_name + ' : ' + e.msg + '. Skipping.')
        except URLError:
            print('Failed to download ' + page_name + '. Trying again in 2 seconds')
            time.sleep(2)
            if attempt > 8:
                print('You might have lost internet connection.')
                print('Breaking out of loop and committing')
                commit_to_database()
                sys.exit()

    return response


def get_tmdb_movie_id(movie):
    if len(movie['imdb_id']) != 9:
        raise ValueError("Movie have no IMDB ID. Skipping.")

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
    if movie['imdb_id'] is None:
        movie['imdb_id'] = tmdb_movie_metadata['imdb_id']
    response.close()


def get_secondary_tmdb_movie_metadata(movie):
    global secondary_tmdb_movie_metadata

    response = retrieve_web_page('https://api.themoviedb.org/3/movie/'
                                 + movie['tmdb_id'] +
                                 '?api_key=' + tmdb_api_key +
                                 '&language=' + secondary_language, 'Secondary language movie metadata from tmdb')
    secondary_tmdb_movie_metadata = json.loads(response.read().decode('utf-8'))
    if movie['imdb_id'] is None:
        movie['imdb_id'] = tmdb_movie_metadata['imdb_id']
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


def add_to_insert_commit_list(commit_list, entry_id, key, value):
    if entry_id in commit_list:
        commit_list[entry_id][key] = value
    else:
        commit_list[entry_id] = {key: value}


def delete_collection(collection, locks_limit=0):
    if 'metadata_id' in collection:
        cursor.execute('SELECT id, [index], user_fields '
                       'FROM metadata_items '
                       'WHERE metadata_type = 18 '
                       'AND library_section_id = ? '
                       'AND id = ?', (library_key, collection['metadata_id'],))
    else:
        cursor.execute('SELECT id, [index], user_fields '
                       'FROM metadata_items '
                       'WHERE metadata_type = 18 '
                       'AND library_section_id = ? '
                       'AND title = ?', (library_key, collection['title'],))
    for item in cursor.fetchall():
        if not len(item[2].split('|')) > locks_limit:
            continue
        delete_commits.append([item[0], item[1]])


def tool_remove_empty_collections():

    cursor.execute('SELECT id, [index] '
                   'FROM metadata_items ' 
                   'WHERE metadata_type = 18 '
                   'AND library_section_id = ?', (library_key,))
    for collection_id in cursor.fetchall():
        collection = {'metadata_id': collection_id[0]}
        cursor.execute('SELECT id '
                       'FROM taggings '
                       'WHERE tag_id = ?', (collection_id[1],))
        if cursor.fetchone() is None:
            delete_collection(collection)


def tool_remove_unlocked_collections():

    cursor.execute('SELECT id, [index], user_fields '
                   'FROM metadata_items '
                   'WHERE metadata_type = 18 '
                   'AND library_section_id = ?', (library_key,))
    for collection_id in cursor.fetchall():
        collection = {'metadata_id': collection_id[0]}
        if collection_id[2] is None \
                or collection_id[2] == '' \
                or collection_id[2] == 'lockedFields=':
            delete_collection(collection)


if config['TOOLS'].getboolean('delete_collections_no_movies'):
    tool_remove_empty_collections()
if config['TOOLS'].getboolean('delete_collections_no_locks'):
    tool_remove_unlocked_collections()
if not (config['TOOLS'].getboolean('delete_collections_no_locks')
        or config['TOOLS'].getboolean('delete_collections_no_movies')):
    main()
else:
    commit_to_database()

sys.exit()
