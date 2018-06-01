# PlexUnify
intro
----------
### Linux installation:

download the PlexUnify.py file and put it somewhere. for example:
```sh
    cd /opt && git clone https://github.com/KBlixt/PlexUnify.git && cd PlexUnify
```

you'll also need 2 python packages:
```sh
    python3 pip plexapi
    python3 pip sqlite3
```

Now you'll need to configure the config file. There is a file called config.cfg-example, use it as a template.
All configuration options are documented and it should be fairly straight forward how to fill it all in.
The file is fairly long, but it's set sane settings. Just enabling a section should be good enough for most.
if you need further help look in the configuring section. Name the file "config.cfg " and you should be ready to go!

Unless you've specifically disabled it, the script will ask for permission to write to the database
before it writes anything. So make sure to run it in a terminal.
The code will run the smoothest if you run it as the user that runs plex. this should avoid any permission troubles.
```sh
    sudo -u plex python3 PlexUnify.py
```

The script is not designed to be run automatically. But, I've writen to the database while plex have been running
several times. as long as you restart plex after the script you should be fine.
----------

### Other installations:

#### for windows and mac:
I have no real experience with running python on these systems. But as long as you configure the config file correctly
and install the packages "plexapi" and "sqlite3" to python3 and run the script in a terminal using python3 you guys
should be set. if you gus run into trouble just raise an issue and I'll look into it if I can as long as it isn't
general python issues.
----------

#### for NAS and other OS:
I really have no clue. if you setup the config file and install the python packages "plexapi" and "sqlite3" to python3
somehow the script should run as long as you can run python3 code in a terminal.
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

# I'm not sure



## FAIR WARNING:

This script will edit the PlexMediaServer database directly! Now, that being said. I've worked and abused my databsae quite
extensively and not run into problems yet. and should you run into some serious issues you can replace the database
with a backup.
----------
