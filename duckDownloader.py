#!/usr/bin/env python3

# Builtin libraries
import json
import logging
import pathlib
import requests
import time
import typing
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

# Dependent libraries
import pandas as pd

# Please do not modify prior to this point unless you know what you're doing
##### START OF CONFIG #####

# Can drop .DEBUG down to .INFO or .ERROR for smaller file size, but there will
# be less help for you/me diagnosing bugs. Up to you!
LOGGING_LEVEL = logging.DEBUG

# Where to store/search for previously saved metadata and logs (relative to
# script execution location!)
DEFAULT_METADATA_PATH = "urls.csv"
DEFAULT_LOGFILE_PATH  = "duck.log" # Set to None (without quotes) to log to terminal
TRANSCRIPT_PATH       = "transcripts" # Directory name (will be created if necessary)

# Minimum time between requests to the same domain. Please be kind to Duck's site.
RATELIMIT_SECONDS = 0.05

##### END OF CONFIG #####
# Please do not modify beyond this point unless you know what you're doing

# Members transcripts ARE NOT indexed publicly, but you can fetch the metadata
# for them (NotImplemented)
MEMBERS_DATA = "https://raw.githubusercontent.com/duckautomata/dokiscripts-data/refs/heads/master/yt-dlp-archive-members.txt"
DOKISCRIPTS_URL = "https://raw.githubusercontent.com/duckautomata/dokiscripts-data/refs/heads/master/yt-dlp-archive-regular.txt"


logger = logging.getLogger(__name__)


class RateLimitedDomainRequest():
    """
        Assigns a simple map of URL domains to last-requested-time and enforces
        a minimum delay between repeated requests on a per-domain basis.
    """
    def __init__(self,
                 ratelimit_seconds: float,
                 ):
        self.domains = dict() # [str, float]
        self.ratelimit_seconds = ratelimit_seconds

    def request(self,
                url: str,
                ) -> requests.Response:
        # Requests are limited based on the domain we are talking to
        domain = urlparse(url).netloc
        # If never spoken to before, set default to omit any sleeping
        last_pinged = self.domains.setdefault(domain,
                                              time.time()-self.ratelimit_seconds)

        # Sleep (hopefully just once) to respect ratelimit as expressed in seconds
        while (sleep_remain :=
                -1*((time.time() - last_pinged) - self.ratelimit_seconds)
               ) > 0.0:
            time.sleep(sleep_remain)
            logger.debug(f"Slept {sleep_remain}s to respect "
                         f"{self.ratelimit_seconds} ratelimit for domain "
                         f"'{domain}'")
        resp = requests.get(url)
        # Log last-pinged time for future requests to be limited appropriately
        self.domains[domain] = time.time()
        return resp

rateLimiter = RateLimitedDomainRequest(ratelimit_seconds = RATELIMIT_SECONDS)

# TODO: Can fetch srt (or thumbnail image .webp, if cached) directly from archive as:
#  YYYYMMDD - StreamType - StreamName - [id]
#  Regex "^(\d{8}) - (.+?) - (.+) - \[([^\]]+)\]\.srt$" (or .webp for image)
# URL base: https://raw.githubusercontent.com/duckautomata/dokiscripts-data/refs/heads/master/Transcript/{STREAMER}/{REGEX}
# Works for both webp and srt

def pandas_append_series_to_end_of_frame(df: pd.DataFrame,
                                         se: pd.Series,
                                         ) -> pd.DataFrame:
    """
        There's probably a better way to do this, but this pattern shows up a
        lot and it is ugly AF
    """
    return pd.concat((df,
                      pd.DataFrame(se).T.set_index([pd.Index([len(df)])]),
                      ))

def ratelimited_duck_request(meta_id: str,
                             kind: str,
                             ) -> Tuple[Union[Dict|str], int]:
    """
        Fetch the requested metadata or transcript by ID while respecting
        global rate limits; return the JSON data for parsing and request status
        (in case of error)

        kind should be "metadata" for metadata only or "transcript" for
        transcript data
    """
    # Select metadata endpoint for smaller payload at first, also prevents
    # us from downloading in-process stream transcripts (incomplete!)
    if kind == 'metadata':
        TRANSCRIPT_URL = f"https://archive.dokiscripts.com/stream/{meta_id}"
    elif kind == 'transcript':
        TRANSCRIPT_URL = f"https://archive.dokiscripts.com/transcript/{meta_id}"
    else:
        raise ValueError(f"Parameter 'kind' must be in ['metadata',"
                         f"'transcript'] (value: '{kind}')")

    # Requests library/rateLimiter handle logging just fine
    duck_response = rateLimiter.request(TRANSCRIPT_URL)
    # Caller will determine what to do with return/bad codes
    if duck_response.status_code != 200:
        return duck_response.text, duck_response.status_code
    else:
        return duck_response.json(), duck_response.status_code

def merge_OK(known: pd.DataFrame,
             added: pd.DataFrame,
             failed: pd.DataFrame,
             ) -> pd.DataFrame:
    """
        Safely combine / update known dataframe while handling failures
        Return the updated known dataframe

        TODO: Could refactor this and fetch_duck() to provide/consume
        generators, ensuring that metadata is consistent on-disk with latest
        execution in the event of an interrupt
    """
    if len(failed) > 0:
        logger.warning(f"Failed to locate {len(failed)} transcripts")
    for (rowidx, row) in added.iterrows():
        lookup = known['id_path'] == row['id_path']
        if lookup.sum() > 0:
            # Update in-place
            idx = lookup.tolist().index(True)
            known.loc[idx,:] = row
        else:
            # Append new row
            known = pandas_append_series_to_end_of_frame(known, row)
    logger.info(f"Merged {len(added)} new transcripts into metadata")
    # Save to disk NOW
    store_known(known, DEFAULT_METADATA_PATH)
    return known

def make_transcript(record: pd.Series,
                    override_local: bool = False,
                    ) -> pd.Series:
    """
        Download a transcript to satisfy a metadata request, preferring local
        copies unless forcibly overridden
    """
    # Check for local path first
    transcript_path = pathlib.Path(TRANSCRIPT_PATH) / f"{record['id_path']}.txt"
    transcript_path.parents[0].mkdir(parents=True, exist_ok=True)

    if not override_local and transcript_path.exists():
        logger.info(f"{transcript_path} already exists -- previously downloaded"
                     "but metadata lost?")
    else:
        duckData, status = ratelimited_duck_request(record['id_path'],
                                                    kind='transcript')
        if status != 200:
            logger.error("Unable to make transcript due to request code "
                         f"{status} from Duck Archive for ID {row['id_path']}")
            # Propagate error to caller to omit this record for now
            raise ValueError
        transcript_json = duckData['transcriptLines']
        with open(transcript_path, 'w') as f:
            if transcript_json is None:
                # Some videos do not have spoken words to transcribe, giving
                # null response (None in Python). I use this particular string
                # rather than an empty file to demonstrate that the download
                # occurred but no transcript content was provided
                f.write("--NULL TRANSCRIPT: NO YAPPING DETECTED--\n")
            else:
                for line in transcript_json:
                    f.write(f" [{line['start']}] {line['text']}"+"\n")
    # Denote that the record is available for future runs of the script
    record['downloaded'] = True
    return record

def fetch_duck(missed: pd.DataFrame,
               ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
        Use Duck's website to convert IDs of missing data into properly
        metadata-tagged and locally downloaded transcripts.
        If the download fails etc, return it in the failed dataframe
    """

    cols = missed.columns

    additions = pd.DataFrame(columns=cols)
    failures = pd.DataFrame(columns=cols)

    # Remap between Duck Transcript JSON and my metadata format
    DuckRemapKeys = {
            'streamer': 'user',
            'date': 'date',
            'streamType': 'type',
            'streamTitle': 'title',
            'id': 'id_path',
            #transcriptLines is NOT remapped! and ONLY available via the
            #                                 /transcripts endpoint
            }

    for (rowidx, row) in missed.iterrows():
        # If metadata already exists, no need to ping for updates
        if row.isna().sum() == 0:
            logger.info(f"Metadata already provided for {row.tod_dict()}, no "
                        "ping to Duck's site")
        else:
            logger.info(f"Fetch metadata for {row.to_dict()}")
            duckData, status = ratelimited_duck_request(row['id_path'],
                                                        kind='metadata')
            if status != 200:
                logger.error("Unable to make metadata due to request code "
                             f"{status} from Duck Archive for ID "
                             f"{row['id_path']}")
                failures = pandas_append_series_to_end_of_frame(failures, row)
                continue
            try:
                new_record = pd.Series(index=cols, dtype=object)
                # Metadata-only at first
                new_record['downloaded'] = False
                for key in duckData.keys():
                    if key in DuckRemapKeys and \
                       DuckRemapKeys[key] in new_record.index:
                        new_record[DuckRemapKeys[key]] = duckData[key]
                    else:
                        raise KeyError(key)
            except Exception as e:
                logger.error("Failed to create metadata record for stream "
                             f"{row['id_path']}: {e}")
                if hasattr(e, 'msg'):
                    logger.error(f"Exception Message: {e.msg}")
                failures = pandas_append_series_to_end_of_frame(failures, row)
                continue
        # Fetch transcript
        try:
            new_record = make_transcript(new_record)
        except Exception as e:
            logger.error("Failed to create record for transcript "
                         f"{row['id_path']}: {e}")
            if hasattr(e, 'msg'):
                logger.error(f"Exception Message: {e.msg}")
            failures = pandas_append_series_to_end_of_frame(failures, row)
            continue
        # Add record to additions
        additions = pandas_append_series_to_end_of_frame(additions, new_record)
    return additions, failures

def missing(known: pd.DataFrame,
            kind: str
            ) -> pd.DataFrame:
    """
        Compare known metadata to possibly fetch-able data of given kind.
        Return a NEW dataframe with fetch-able IDs for new data
    """
    # NOTE: Duck uses the following as metadata IDs:
    #  - Youtube: youtube.com/watch?v=XXXXXXXX part of URL
    #  - Twitch/TwitchVOD: The Twitch stream ID (ie: v##########)
    #                      OR the YouTube video ID from Twitch VOD channel
    #  - 'External' videos may come from other channels, but are generally from
    #               youtube and therefore follow the YouTube schema
    need_download = known[(~known['downloaded']) & (known['type'] == kind)]
    if len(need_download) > 0:
        logger.info(f"Found {len(need_download)} {kind} transcripts that "
                    "require download")
    return need_download

def inject_dokiscripts_data(known: pd.DataFrame,
                            ) -> pd.DataFrame:
    """
        Fetch the latest mapping of known dokiscripts data for Duck's archives
        This is a roundabout way that is PROBABLY OK to be up-to-date, but
        might not be ideal
    """
    duck_response = rateLimiter.request(DOKISCRIPTS_URL)

    if duck_response.status_code != 200:
        logger.error("Failed to retrieve updated DokiScripts data (Error: "
                     f"{duck_response.status_code})")
        return known

    archive_list = duck_response.text.split('\n')
    # Purge any empty lines (usually just one at the end, but be thorough)
    try:
        while (to_delete := archive_list.index('')):
            del archive_list[to_delete]
    except ValueError: # We're done
        pass
    logger.info(f"Retrieved a total of {len(archive_list)} entries from "
                "DokiScripts")
    original_length = len(known)

    # Compare to known data
    could_retrieve = set([_.split(' ')[1] for _ in archive_list])
    already_have = set(known['id_path'].tolist())
    new_transcripts = list(could_retrieve-already_have)
    # Make new entries in known data
    new_df = pd.DataFrame(columns=known.columns)
    # Identity remaps will be handled by setdefault, only twitchvod->twitch is
    # known as non-identity remap
    DuckTypeRemap = {'twitchvod': 'twitch'}
    for line in archive_list:
        duckType, duckId = line.split(' ')
        if duckId in new_transcripts:
            new_series = pd.Series(index=known.columns, dtype=object)
            new_series['type'] = DuckTypeRemap.setdefault(duckType, duckType)
            new_series['id_path'] = duckId
            new_series['downloaded'] = False
            known = pandas_append_series_to_end_of_frame(known, new_series)
    new_length = len(known)
    logger.info(f"Added {new_length-original_length} transcript candidates "
                "from DokiScripts to retrieve")
    return known

def fetch_all_missing(known: pd.DataFrame,
                      ) -> pd.DataFrame:
    """
        Fetch missing IDs per source, and attempt to load the data for these
        sources. If any new data is loaded, add it to the known record metadata
    """
    logger.info("Fetch dokiscripts-data yt-dlp-archive metadatas")
    known = inject_dokiscripts_data(known)

    # While technically we could fetch all sources at once, I am splitting them
    # based on Duck's streamType identifier in case the API changes in the future
    for source in ['youtube','twitch','external']:
        logger.debug(f"Fetch {source.capitalize()} Missing")
        known = merge_OK(known, *fetch_duck(missing(known, source)))
    return known

def load_known(csv: Union[str,pathlib.Path],
               ) -> pd.DataFrame:
    """
        Safely load the known metadata from str/pathlike and return as DF
    """
    if not isinstance(csv, pathlib.Path):
        csv = pathlib.Path(csv)
    if not csv.exists():
        logger.info(f"Did NOT find csv at {csv}, making empty template")
        df = pd.DataFrame(columns=['date','user','type','title','downloaded',
                                   'id_path'])
        return df

    df = pd.read_csv(csv)
    logger.info(f"Loaded {len(df)} records from '{csv}'")
    return df

def store_known(csvdata: pd.DataFrame,
                path: Union[str,pathlib.Path],
                ) -> None:
    """
        Store updated metadata after program execution
    """
    csvdata.to_csv(path, index=False)

def main() -> None:
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s",
                        filename=DEFAULT_LOGFILE_PATH,
                        level=LOGGING_LEVEL,
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger.info(f"Load metadata from '{DEFAULT_METADATA_PATH}'")
    known = load_known(DEFAULT_METADATA_PATH)
    fetch_all_missing(known)

if __name__ == "__main__":
    main()

