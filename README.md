# PlexUnify

The script can do quite a few things, here is a list of the stuff it can do with a short description.

- Add taglines to movies that miss them if they exist on tmdb. 
- Add imdb content ratings and rename them.
- Rename and merge genres.
- Edit the original title to include the title of your language.
- Add movies to collections.
- Add collection posters and art based on the poster and art the collection have on tmdb.
- Add collection posters and art based on the poster and art of the movies in the collection.
- Add content rating to collections.
- Add summaries to collections.
- Create new collections based on a filter (filtering based on the quantity and quality of movies in a collection)


It also includes a few tools that are completely separate which if enabled will not run the other part of the script.

- Delete all movie collections that are empty.
- Delete all movie collections that have 0 edited fields (orange lock) 

A collection falling into one of these description is either useless or probably auto generated. but use with care anyway.

PlexUnify is a script that edits the plex database directly. but I've used it quite extensivly and even missused it. runnig it 



----------
## Linux installation:

download the PlexUnify.py file and put it somewhere. for example:
```sh
cd /opt && git clone https://github.com/KBlixt/PlexUnify.git && cd PlexUnify
```

you'll also need two python3 packages:
```sh
python3 pip -m install plexapi
```

Now you'll need to configure the config file. There is a file called config.cfg-example, use it as a template.
All configuration options are documented and it should be fairly straight forward how to fill it all in.
The file is fairly long, but it's set sane settings. Just enabling a section should be good enough for most.
if you need further help look in the configuring section. Name the file "config.cfg " and you should be ready to go!

Unless you've specifically disabled it, the script will ask for permission to write to the database
before it writes anything. So make sure to run it in a terminal.
The code will run the smoothest if you run it as the user that runs plex. this should avoid any permission troubles.
So make sure that the plex user actually can access the script.
```sh
sudo chmod -R 755 /opt/PlexUnify
sudo -u plex python3 PlexUnify.py
```

the script is split into two parts, the information gathering/processing part and the writing part. the script will run through
all the information and once that's done it will write all at once, the write part is done in a fraction of a second so plex 
downtime should be minimal. If you want the script to have the ability to generate new collections you'll need to have
plex available to the plexapi while the information gathering/processing part is running.

The script is not designed to be run automatically. But, I've writen to the database while plex have been running
several times. as long as you restart plex after the script you should be fine.

the script is not super fast. depending on how manny api requests you need to make per movie it'll take somewhere between
2-6 seconds per movie depending on internet connection and other things. If you only use things that don't requires to
download anything, then it'll be quite fast maybe 0.1 sec per movie.

----------

## Other installations:

#### for windows and mac:

I have no real experience with running python on these systems. But as long as you configure the config file correctly
and install the packages "plexapi" and "sqlite3" to python3 and run the script in a terminal using python3 you guys
should be set. if you gus run into trouble just raise an issue and I'll look into it if I can as long as it isn't
general python issues.

----------

#### for NAS and other OS:

I really have no clue. if you setup the config file and install the python packages "plexapi" and "sqlite3" to python3
somehow the script should run as long as you can run python3 code in a terminal.

please let me know if it works/ doesn't work for you.

----------

### Config help:

If you don't know where the plex home directory is then you've probably not moved it and you can look it up [here](https://support.plex.tv/articles/202915258-where-is-the-plex-media-server-data-directory-located/)
You'll need an tmdb api key for pretty much everything in this script. [here is a good guide](https://developers.themoviedb.org/3/getting-started/introduction)
You'll also need some tmdb language codes. either look them up or just wing it. [language-code]-[country-code] is a good bet.


using en-US as the secondary language is recommended as it's the most reliable language on tmdb. if en-US is your main
language. well, then most of the language based stuff in this script should be useless to you anyway. just set both to
en-US and there will be no harm done.

Everything is disabled by default, So you won't do anything you've not actively chosen to use.

----------
### Notes.

I'm not actually deleting collections, I'm just moving them out of the way so they can't be seen. this is done because the 
database dislikes deleting stuff in that specific table. removing tags and removing tags from movies is just fine but not 
the metadata itself. for all intents and purposes they are gone though. if I learn a way to remove these entries from the 
database it's easily patched in.

not using the "sort title" for a similar reasons that i'm using the "original title" field have the exact same reason. editing 
that specific column in that specific table seems to be an issue. no idea why, but a warning pops up whenever I try to do it so 
I left it alone... 

...of course i didn't. I tried to change the stuff anyway. Nothing happened but for safety I left it out of the script. while it
might not have any short term damage who knows if this stuff cause damage in the long term.

## FAIR WARNING:

This script will edit the PlexMediaServer database directly! Now, that being said. I've worked and abused my database quite
extensively and not run into problems yet. and should you run into some serious issues you can replace the database
with a backup.

----------
