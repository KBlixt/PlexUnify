import codecs
import sys
import os
import configparser
import time
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

def main():
    process_movie(library.get('Cars'))
    pass


def process_movie(movie):
    pass


    # change tags
    change_genre(movie)

    # add tagline

    # unify content_rating

    # add second language to title_sort


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


def change_genre(item):
    for change_to, change_from_string in config.items('TAG_CHANGES'):
        change_from_list = change_from_string.split(',')
        for change_from in change_from_list:
            for genre in item.genres:
                if genre.tag.lower().capitalize() == change_from.lower().capitalize():
                    item.removeGenre(change_from.lower().capitalize())
                    item.reload()
                    item.addGenre(change_to.lower().capitalize())
                    item.reload()

    pass


def add_tagline():
    pass


def unify_contet_rating():
    pass


def add_second_language_to_title_sort():
    pass


main()
