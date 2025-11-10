# Dependencies
import pandas as pd
import tqdm

# Builtins
import argparse
from collections import defaultdict
import re

# Load this pre-args to fill out possible names
metadata = pd.read_csv('urls.csv')

def update_dictionary(dictwords, filename):
    text = re.compile('[a-z]+')
    with open(f'transcripts/{filename}.txt', 'r') as f:
        for line in f.readlines():
            if ']' not in line:
                continue
            words = text.findall(line.split('] ',1)[1].rstrip().lower())
            for word in words:
                dictwords[word] += 1
    return dictwords

def regexwordlookup(regex, worddict):
    search = re.compile(regex)
    accepted = [_ for _ in worddict.keys() if search.match(_)]
    if len(accepted) == 0:
        print(f"No words match regex '{regex}'")
    for word in sorted(accepted):
        print(f"{word}: {worddict[word]}")

def wordlookup(word, worddict):
    if word in worddict:
        print(worddict[word])
    else:
        print(f"No known uses of the word '{word}'")

def countlookup(count, countdict):
    if count in countdict:
        print(countdict[count])
    else:
        print(f"No words with {count} uses")

def build():
    prs = argparse.ArgumentParser(description="A script to count words (or find words spoken a given number of times) by a streamer from transcripts")
    prs.add_argument('user', type=str, choices=sorted(set(metadata['user'])),
                     help="Select transcripts belonging to this user")
    prs.add_argument('--regexify', action='store_true',
                     help="Use regex matching rather than direct matching")
    prs.add_argument('--word', type=str, default=None, nargs='*',
                     help="Word to look up (direct match)")
    prs.add_argument('--count', type=int, default=None, nargs='*',
                     help="Count of word to look up")
    return prs

def parse(args=None, prs=None):
    if prs is None:
        prs = build()
    if args is None:
        args = prs.parse_args()
    return args

def main(args):
    dokistreams = metadata[metadata['user'] == args.user]['id_path']

    words = defaultdict(int)
    for stream in tqdm.tqdm(dokistreams, desc=f"Loading word counts from local transcripts"):
        words = update_dictionary(words, stream)

    if args.regexify:
        wordlookup = regexwordlookup

    if args.word is not None:
        for word in args.word:
            wordlookup(word, words)

    count_to_words = dict((count,list()) for count in sorted(set(words.values())))
    for key, count in words.items():
        count_to_words[count].append(key)

    if args.count is not None:
        for count in args.count:
            countlookup(count, count_to_words)

    if args.word is not None or args.count is not None:
        return

    print(f"Use CTRL+C or ENTER without input to quit")
    prompt = "Enter a word to see its count or an integer to see all words with that count: "
    while True:
        request = input(prompt)
        if request == '':
            break

        try:
            int_request = int(request)
        except ValueError:
            int_request = None

        if int_request is not None:
            countlookup(int_request, count_to_words)
        else:
            wordlookup(word, words)

if __name__ == '__main__':
    args = parse()
    main(args)

