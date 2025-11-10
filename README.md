# Duck Downloader - A Companion Utility for Managing Offline copies of DokiScripts

You may directly make a copy of Duck's Transcripts [at his repository](https://github.com/duckautomata/dokiscripts-data/).
It is the source of truth for this repository, so you may be better served by checking THERE first.

This repository exists because I wish to keep my own local copies in a slightly different format, which is easier to achieve by copying the final artifacts from DokiScripts than running a fork myself.

# Setup

The Python script relies on local installations of Python3 and the Pandas library.
Refer to [https://www.python.org/downloads/]() and [https://pypi.org/project/pandas/]() for instructions on installing the software on your system.
To download Pandas via your Python interpreter, you may instead `pip install -r requirements.txt`.
Note that the `requirements.txt` file lists a _suggested_ minimum version, but is _not strictly tested_ against all previous/future versions.
This generally requires Python 3.9 and later, but again your mileage may vary: and newer/older versions of Python3 are _not strictly tested_.

# Configuration

**No configuration is required**, but you can tweak a few settings to your liking by editing [duckDownloader.py](./duckDownloader.py).

Refer to the comments `##### START OF CONFIG #####` and `##### END OF CONFIG #####` as the start/stop of the configurable list of options.
Further details about the configurable behaviors are provided in comments, but in short this allows you to:
* Change the file paths for logs, transcripts and metadata
* Adjust the verbosity of logging
* Change the rate limit for downloads (please be kind to Duck's website)

NOTE: A fresh run under default configurations may be expected to take about 13 minutes to complete.
Future executions will not require as much time (ie: <1 second if no new transcripts are available), as your first download is pulling ALL past transcripts and these will not need to be re-downloaded.
You can follow the "duck.log" to see the script's progress as it runs, ie: `tail -F duck.log`.

# Execution

Navigate to the file in your terminal of choice and simply execute `python3 duckDownloader.py`.

If you'd like to have the script run once daily, you can modify the `<PATH/TO/DuckDownloader>` in [suggested_crontab](./suggested_crontab) and schedule it in your crontab, but make sure you know what you're doing.

# Tools

Programs under the tools directory will interact with your local archives to perform various fun tasks.
All tools are designed to be executed from the repository's top-level directory, ie: `python3 tools/count_words.py`
Use the `--help` argument for descriptions and options of each script.

# In the Future (maybe)

* Filter downloads by metadata matches such as: streamer, date range, title-regex
* Pull thumbnails into /thumbnails
* I'd love to provide a "double-click to run" method for Windows/Mac, but don't count on it
* Crontab-like solutions for other systems

