import sys
import sqlite3
import os

class PlexDatabaseInterface:

    def __init__(self, database_dir):

        self.database = sqlite3.connect(database_dir)
        self.cursor = self.database.cursor()

    def get_data_helper(self, query_list, table, conditions=''):

        query = ', '.join(query_list)

        command = 'SELECT ' + query + ' FROM ' + table + ' ' + conditions

        if command.endswith(' '):
            command = command[:-1]
        data = list()
        for response in self.cursor.execute(command):

            data.append(dict(zip(query_list, response)))

        return data

    def put_data_helper(self, data_dict, table, conditions):

        command = 'UPDATE ' + table + ' SET'

        for column_name, value in data_dict:
            command += ' ' + column_name + ' = ' + value

        command += ' ' + conditions

        self.cursor.execute(command)

        return True

    def get_metadata(self, ):
        table = 'metadata_items'
        query = ['*']


db = PlexDatabaseInterface('PlexDatabase.db')

print('end')