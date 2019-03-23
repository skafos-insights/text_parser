import itertools
from itertools import zip_longest
import json
import os
from pathlib import Path
import re
import requests
import scipy
from typing import List, Tuple

import numpy as np
import pandas as pd
import sklearn as sk
import tika

BASE_URL = "https://thoughtful-confused-italiangreyhound.gigalixirapp.com/api"
MINUTES_PATH = Path('../data/cville_pdfs/minutes/').glob('*.txt')

# This is awful and hacky -- I extracted it from the metadata of these PDFs historically.
FILE_NAMES_AND_DATES = dict([
    ('../data/cville_pdfs/minutes/64318.pdf', '2019-02-04T15:22:02Z'),
    ('../data/cville_pdfs/minutes/64316.pdf', '2019-02-04T15:27:17Z'),
    ('../data/cville_pdfs/minutes/64314.pdf', '2019-01-29T15:10:18Z'),
    ('../data/cville_pdfs/minutes/64312.pdf', '2019-01-29T14:19:56Z'),
    ('../data/cville_pdfs/minutes/64246.pdf', '2019-01-29T22:37:55Z'),
    ('../data/cville_pdfs/minutes/64244.pdf', '2019-01-29T22:38:55Z'),
    ('../data/cville_pdfs/minutes/64242.pdf', '2019-01-29T22:39:22Z'),
    ('../data/cville_pdfs/minutes/64205.pdf', '2019-01-15T17:50:10Z'),
    ('../data/cville_pdfs/minutes/64203.pdf', '2019-01-15T17:37:01Z'),
    ('../data/cville_pdfs/minutes/64071.pdf', '2018-12-31T21:07:07Z'),
    ('../data/cville_pdfs/minutes/64069.pdf', '2019-01-06T20:29:54Z'),
    ('../data/cville_pdfs/minutes/64067.pdf', '2018-12-31T21:07:35Z'),
    ('../data/cville_pdfs/minutes/63926.pdf', '2018-12-20T19:29:07Z'),
    ('../data/cville_pdfs/minutes/63924.pdf', '2018-12-20T19:20:48Z'),
    ('../data/cville_pdfs/minutes/63922.pdf', '2018-12-20T19:20:21Z'),
    ('../data/cville_pdfs/minutes/63920.pdf', '2018-12-12T19:05:48Z'),
    ('../data/cville_pdfs/minutes/63751.pdf', '2018-12-05T14:35:22Z'),
    ('../data/cville_pdfs/minutes/63749.pdf', '2018-11-25T20:31:49Z'),
    ('../data/cville_pdfs/minutes/63645.pdf', '2018-11-21T15:32:12Z'),
    ('../data/cville_pdfs/minutes/63643.pdf', '2018-11-13T15:50:24Z'),
    ('../data/cville_pdfs/minutes/63541.pdf', '2018-11-12T17:13:30Z'),
    ('../data/cville_pdfs/minutes/63539.pdf', '2018-11-12T17:11:31Z'),
    ('../data/cville_pdfs/minutes/63537.pdf', '2018-11-12T17:12:12Z'),
    ('../data/cville_pdfs/minutes/63535.pdf', '2018-11-12T17:12:50Z'),
    ('../data/cville_pdfs/minutes/63533.pdf', '2018-11-12T17:10:49Z'),
    ('../data/cville_pdfs/minutes/63531.pdf', '2018-11-21T15:25:44Z'),
    ('../data/cville_pdfs/minutes/63341.pdf', '2018-10-17T15:08:26Z'),
    ('../data/cville_pdfs/minutes/63339.pdf', '2018-10-17T15:09:16Z'),
    ('../data/cville_pdfs/minutes/63337.pdf', '2018-10-17T15:10:00Z'),
    ('../data/cville_pdfs/minutes/63335.pdf', '2018-10-17T15:10:39Z'),
    ('../data/cville_pdfs/minutes/63333.pdf', '2018-10-17T15:11:18Z'),
    ('../data/cville_pdfs/minutes/63331.pdf', '2018-10-17T15:11:48Z'),
    ('../data/cville_pdfs/minutes/63329.pdf', '2018-10-17T15:30:02Z'),
    ('../data/cville_pdfs/minutes/63327.pdf', '2018-10-17T15:29:17Z'),
    ('../data/cville_pdfs/minutes/63325.pdf', '2018-10-17T15:14:24Z'),
    ('../data/cville_pdfs/minutes/63323.pdf', '2018-10-17T14:58:58Z'),
    ('../data/cville_pdfs/minutes/63321.pdf', '2018-10-17T14:57:27Z'),
    ('../data/cville_pdfs/minutes/63319.pdf', '2018-10-17T14:56:59Z'),
    ('../data/cville_pdfs/minutes/63317.pdf', '2018-10-17T14:56:10Z'),
    ('../data/cville_pdfs/minutes/63315.pdf', '2018-10-17T14:55:44Z'),
    ('../data/cville_pdfs/minutes/63313.pdf', '2018-10-17T14:52:48Z'),
    ('../data/cville_pdfs/minutes/62995.pdf', '2018-09-17T19:23:21Z'),
    ('../data/cville_pdfs/minutes/62993.pdf', '2018-09-17T19:22:50Z'),
    ('../data/cville_pdfs/minutes/62991.pdf', '2018-09-17T19:11:02Z'),
    ('../data/cville_pdfs/minutes/62989.pdf', '2018-09-17T19:22:08Z'),
    ('../data/cville_pdfs/minutes/62938.pdf', '2018-09-18T17:07:42Z'),
    ('../data/cville_pdfs/minutes/62932.pdf', '2018-09-11T23:18:47Z'),
    ('../data/cville_pdfs/minutes/62930.pdf', '2018-09-11T23:17:12Z'),
    ('../data/cville_pdfs/minutes/62928.pdf', '2018-09-11T23:15:47Z'),
    ('../data/cville_pdfs/minutes/62371.pdf', '2018-07-12T19:28:55Z'),
    ('../data/cville_pdfs/minutes/62361.pdf', '2018-07-12T17:46:15Z'),
    ('../data/cville_pdfs/minutes/62359.pdf', '2018-07-12T16:23:31Z'),
    ('../data/cville_pdfs/minutes/62357.pdf', '2018-07-12T15:23:52Z'),
    ('../data/cville_pdfs/minutes/62355.pdf', '2018-07-12T15:09:29Z'),
    ('../data/cville_pdfs/minutes/62353.pdf', '2018-07-12T15:03:54Z'),
    ('../data/cville_pdfs/minutes/62351.pdf', '2018-07-12T14:29:16Z'),
    ('../data/cville_pdfs/minutes/62335.pdf', '2018-07-11T18:41:48Z'),
    ('../data/cville_pdfs/minutes/61745.pdf', '2018-05-04T20:53:04Z'),
    ('../data/cville_pdfs/minutes/61743.pdf', '2018-05-04T20:37:23Z'),
    ('../data/cville_pdfs/minutes/61741.pdf', '2018-05-04T19:54:38Z'),
    ('../data/cville_pdfs/minutes/60339.pdf', '2018-02-16T18:57:36Z'),
    ('../data/cville_pdfs/minutes/60337.pdf', '2018-02-16T19:17:08Z'),
    ('../data/cville_pdfs/minutes/60325.pdf', '2018-02-16T21:21:21Z'),
    ('../data/cville_pdfs/minutes/60199.pdf', '2018-02-12T22:45:15Z'),
    ('../data/cville_pdfs/minutes/60197.pdf', '2018-02-12T22:42:27Z'),
    ('../data/cville_pdfs/minutes/60169.pdf', '2018-02-12T21:53:15Z'),
    ('../data/cville_pdfs/minutes/59997.pdf', '2018-01-25T22:34:12Z'),
    ('../data/cville_pdfs/minutes/59995.pdf', '2018-01-23T17:03:59Z'),
    ('../data/cville_pdfs/minutes/56212.pdf', '2017-10-05T19:33:14Z'),
    ('../data/cville_pdfs/minutes/56208.pdf', '2017-10-04T19:54:51Z'),
    ('../data/cville_pdfs/minutes/56206.pdf', '2017-10-04T19:24:20Z'),
    ('../data/cville_pdfs/minutes/56202.pdf', '2017-10-04T18:31:24Z'),
    ('../data/cville_pdfs/minutes/56196.pdf', '2017-10-04T18:32:45Z'),
    ('../data/cville_pdfs/minutes/53263.pdf', '2017-06-13T19:24:20Z'),
    ('../data/cville_pdfs/minutes/53261.pdf', '2017-06-13T19:20:51Z'),
    ('../data/cville_pdfs/minutes/53259.pdf', '2017-06-13T19:23:19Z'),
    ('../data/cville_pdfs/minutes/52757.pdf', '2017-05-16T20:10:14Z'),
    ('../data/cville_pdfs/minutes/52755.pdf', '2017-05-17T19:08:24Z'),
    ('../data/cville_pdfs/minutes/52753.pdf', '2017-05-16T19:59:56Z'),
    ('../data/cville_pdfs/minutes/52751.pdf', '2017-05-16T20:01:19Z'),
    ('../data/cville_pdfs/minutes/52749.pdf', '2017-05-16T20:09:02Z'),
    ('../data/cville_pdfs/minutes/52747.pdf', '2017-05-16T20:11:09Z'),
    ('../data/cville_pdfs/minutes/52745.pdf', '2017-05-17T18:11:32Z'),
    ('../data/cville_pdfs/minutes/52368.pdf', '2017-04-18T18:16:56Z'),
    ('../data/cville_pdfs/minutes/52292.pdf', '2017-04-06T17:13:05Z'),
    ('../data/cville_pdfs/minutes/49213.pdf', '2017-01-11T16:49:09Z'),
    ('../data/cville_pdfs/minutes/49100.pdf', '2016-12-30T18:53:29Z'),
    ('../data/cville_pdfs/minutes/49098.pdf', '2016-12-30T18:48:25Z'),
    ('../data/cville_pdfs/minutes/49077.pdf', '2016-12-22T21:59:07Z'),
    ('../data/cville_pdfs/minutes/49075.pdf', '2016-12-22T21:57:56Z'),
    ('../data/cville_pdfs/minutes/49073.pdf', '2016-12-22T21:54:39Z'),
    ('../data/cville_pdfs/minutes/49071.pdf', '2016-12-22T21:13:22Z'),
    ('../data/cville_pdfs/minutes/47355.pdf', '2016-10-17T21:42:20Z'),
    ('../data/cville_pdfs/minutes/47353.pdf', '2016-10-17T21:31:36Z'),
    ('../data/cville_pdfs/minutes/47351.pdf', '2016-10-17T21:24:10Z'),
    ('../data/cville_pdfs/minutes/45480.pdf', '2016-08-24T15:06:09Z'),
    ('../data/cville_pdfs/minutes/44366.pdf', '2016-08-09T20:25:17Z'),
    ('../data/cville_pdfs/minutes/44364.pdf', '2016-08-03T14:05:36Z'),
    ('../data/cville_pdfs/minutes/44362.pdf', '2016-08-03T14:02:16Z'),
    ('../data/cville_pdfs/minutes/41666.pdf', '2016-05-23T17:24:46Z'),
    ('../data/cville_pdfs/minutes/41664.pdf', '2016-05-23T17:23:56Z'),
    ('../data/cville_pdfs/minutes/41662.pdf', '2016-05-23T17:22:41Z'),
    ('../data/cville_pdfs/minutes/41660.pdf', '2016-05-23T17:21:57Z'),
    ('../data/cville_pdfs/minutes/41658.pdf', '2016-05-23T17:20:57Z'),
    ('../data/cville_pdfs/minutes/38848.pdf', '2016-03-11T17:52:29Z'),
    ('../data/cville_pdfs/minutes/38846.pdf', '2016-03-11T17:50:39Z'),
    ('../data/cville_pdfs/minutes/38006.pdf', '2016-01-01T22:26:41Z'),
    ('../data/cville_pdfs/minutes/38004.pdf', '2016-01-31T18:07:40Z'),
    ('../data/cville_pdfs/minutes/38002.pdf', '2016-01-18T18:20:00Z'),
    ('../data/cville_pdfs/minutes/37858.pdf', '2016-02-03T15:09:32Z'),
    ('../data/cville_pdfs/minutes/36383.pdf', '2015-12-08T01:47:44Z'),
    ('../data/cville_pdfs/minutes/36381.pdf', '2017-01-17T20:35:20Z'),
    ('../data/cville_pdfs/minutes/36379.pdf', '2015-11-15T04:41:08Z'),
    ('../data/cville_pdfs/minutes/36377.pdf', '2015-10-30T18:31:14Z'),
    ('../data/cville_pdfs/minutes/36375.pdf', '2015-10-06T16:19:28Z'),
    ('../data/cville_pdfs/minutes/34187.pdf', '2015-09-22T22:41:40Z'),
    ('../data/cville_pdfs/minutes/34185.pdf', '2015-09-09T21:29:28Z'),
    ('../data/cville_pdfs/minutes/34071.pdf', '2015-08-19T12:33:39Z'),
    ('../data/cville_pdfs/minutes/33995.pdf', '2015-07-27T15:24:24Z'),
    ('../data/cville_pdfs/minutes/33819.pdf', '2015-07-07T17:13:29Z'),
    ('../data/cville_pdfs/minutes/33818.pdf', '2015-07-07T18:21:21Z'),
    ('../data/cville_pdfs/minutes/33653.pdf', '2015-06-11T14:04:46Z'),
    ('../data/cville_pdfs/minutes/33651.pdf', '2015-06-22T13:37:31Z'),
    ('../data/cville_pdfs/minutes/33267.pdf', '2015-05-07T13:44:17Z'),
    ('../data/cville_pdfs/minutes/33165.pdf', '2015-04-23T21:13:34Z'),
    ('../data/cville_pdfs/minutes/33090.pdf', '2015-04-14T14:06:09Z'),
    ('../data/cville_pdfs/minutes/32827.pdf', '2015-03-20T20:23:48Z'),
    ('../data/cville_pdfs/minutes/32826.pdf', '2015-03-20T20:26:33Z'),
    ('../data/cville_pdfs/minutes/32506.pdf', '2015-02-25T21:41:01Z'),
    ('../data/cville_pdfs/minutes/32335.pdf', '2015-02-11T15:45:08Z'),
    ('../data/cville_pdfs/minutes/32190.pdf', '2015-01-23T20:18:22Z'),
    ('../data/cville_pdfs/minutes/32188.pdf', '2015-01-30T20:02:26Z'),
    ('../data/cville_pdfs/minutes/31933.pdf', '2014-12-31T22:28:27Z'),
    ('../data/cville_pdfs/minutes/31801.pdf', '2014-12-11T18:27:24Z'),
    ('../data/cville_pdfs/minutes/31549.pdf', '2014-11-17T18:46:51Z'),
    ('../data/cville_pdfs/minutes/31476.pdf', '2014-11-07T21:32:02Z'),
    ('../data/cville_pdfs/minutes/31407.pdf', '2014-10-27T21:06:23Z'),
    ('../data/cville_pdfs/minutes/31256.pdf', '2014-10-07T17:25:08Z'),
    ('../data/cville_pdfs/minutes/31182.pdf', '2014-09-30T21:37:02Z'),
    ('../data/cville_pdfs/minutes/31106.pdf', '2014-09-15T14:26:22Z'),
    ('../data/cville_pdfs/minutes/30495.pdf', '2014-08-21T17:46:54Z'),
    ('../data/cville_pdfs/minutes/30292.pdf', '2014-08-04T20:45:44Z'),
    ('../data/cville_pdfs/minutes/30291.pdf', '2014-08-04T20:49:04Z'),
    ('../data/cville_pdfs/minutes/29876.pdf', '2014-06-17T15:20:42Z'),
    ('../data/cville_pdfs/minutes/29875.pdf', '2014-06-17T15:14:36Z'),
    ('../data/cville_pdfs/minutes/29576.pdf', '2014-05-22T15:54:58Z'),
    ('../data/cville_pdfs/minutes/29516.pdf', '2014-05-14T15:36:08Z'),
    ('../data/cville_pdfs/minutes/29322.pdf', '2014-05-01T13:15:12Z'),
    ('../data/cville_pdfs/minutes/29153.pdf', '2014-04-11T14:12:02Z'),
    ('../data/cville_pdfs/minutes/28955.pdf', '2014-03-25T13:55:54Z'),
    ('../data/cville_pdfs/minutes/28820.pdf', '2014-03-11T20:36:04Z'),
    ('../data/cville_pdfs/minutes/28623.pdf', '2014-02-25T15:49:45Z'),
    ('../data/cville_pdfs/minutes/28613.pdf', '2014-02-21T15:46:59Z'),
    ('../data/cville_pdfs/minutes/28434.pdf', '2014-02-10T20:51:24Z'),
])


# Helper functions

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def get_consent_agenda(text: str) -> List[str]:
    try:
        consent_agenda = re.split(r'\n{1}[a-z]\.\s', text)[1:]  # Get the incidence of ('a.', 'b.', ...)
        consent_agenda[-1] = consent_agenda[-1].split('\n\n')[0]
    except IndexError:
        return []
    return consent_agenda


def get_manager_response(text: str, consent_agenda: str) -> str:
    manager_response = text.split(consent_agenda[-1])[1].split('\nCOMMUNITY MATTERS')[0]
    return manager_response


def get_other_items(text: str) -> List[Tuple[str, str]]:
    known_headings = ['APPROPRIATION', 'RESOLUTION', 'REPORT', 'ORDINANCE', 'PUBLIC HEARING', 'A RESOLUTION']
    perms = list(itertools.permutations(known_headings, 2))
    headings = list(itertools.chain.from_iterable([  # Minutes are very lax about how they join together
        map(lambda x: '/'.join(x), perms),
        map(lambda x: ' / '.join(x), perms),
        map(lambda x: '/ '.join(x), perms),
        map(lambda x: ' /'.join(x), perms),
    ]))
    headings.extend(known_headings)
    potential_headings = ['\n' + heading for heading in headings]
    regex_string = '(' + '|'.join(potential_headings) + ')'
    other_items = re.split(regex_string, text)[1:]
    labeled_other_items = list(grouper(other_items, 2, 'xxx'))
    try:
        labeled_other_items[-1] = (labeled_other_items[-1][0], labeled_other_items[-1][1].split('OTHER BUSINESS')[0])
    except IndexError:
        return []
    return labeled_other_items


def replace_newlines(text: str) -> str:
    return text.replace('\n', '')


def replace_terrible_strings(text: str) -> str:
    return text.replace('\(2nd', '').replace('(carried)', '').replace('(CARRIED)', '').replace('(Carried)', '').strip()


def extract_dollar_amount(text: str) -> str:
    pat = re.compile(r'\$\d+\,*\d*\.*\d*')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    else:
        return ''
    
def extract_status(text: str) -> str:
    pat = re.compile(r'\([a-z]+.*\)')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    pat = re.compile(r'\(\d+[a-z].*\)')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    else:
        return ''
    

def extract_voting(text: str) -> str:
    pat = re.compile(r'\(Ayes.*\)')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    return ''
    

def extract_all_caps(text: str) -> str:
    pat = re.compile(r'(([A-Z]|\s)+)')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    

def extract_ayes(text: str) -> str:
    pat = re.compile(r'\(Ayes:.*;')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0].split(': ')[1]
    return ''
    

def extract_noes(text: str) -> str:
    pat = re.compile(r'Noes:.*\.')
    if pat.findall(str(text)):
        return pat.findall(str(text))[0].split(': ')[1]


# Driver functions

def get_consent_agenda_df(text: str, date: str) -> pd.DataFrame:
    """
    Get all the consent agenda items out into a parsed format.
    """
    try:
        consent_agenda = get_consent_agenda(text)
        consent_df = pd.DataFrame(consent_agenda, columns=['raw_text'])
        consent_df['name'] = consent_df['raw_text'].str.split('\n\n').apply(lambda x: x[0])
        consent_df = consent_df[~consent_df['name'].str.contains('Minutes')]
        consent_df['type'] = consent_df['name'].str.replace('\n', '').str.split(': ').apply(lambda x: x[0])
        consent_df['name'] = consent_df['name'].str.replace('\n', '').str.split(': ').apply(lambda x: x[1:]).apply(lambda x: ' '.join(x))
        consent_df['dollar_amount'] = consent_df['name'].apply(extract_dollar_amount)
        consent_df.loc[consent_df['dollar_amount'] == '', 'dollar_amount'] = consent_df['raw_text'].apply(extract_dollar_amount)
        consent_df['name'] = consent_df.apply(lambda x: x['name'].replace(x['dollar_amount'], ''), axis=1).str.replace('-', '')
        consent_df['status'] = consent_df['raw_text'].apply(extract_status)
        consent_df['name'] = consent_df.apply(lambda x: x['name'].replace(x['status'], ''), axis=1)
        consent_df['name'] = consent_df['name'].str.replace('(2nd', '', regex=False).str.replace('(carried)', '', regex=False).str.replace('(CARRIED)', '', regex=False).str.replace('(Carried)', '', regex=False).str.strip()
        consent_df['voting'] = consent_df['raw_text'].apply(extract_voting)
        consent_df['date'] = str(date)
        consent_df['name'] = consent_df['name'].str.split().apply(lambda x: ' '.join(x))
        consent_df = consent_df[consent_df['name'].str.len() < 200]
    except (AttributeError, IndexError):
        return pd.DataFrame([])
    return consent_df


def get_other_items_df(text: str, date: str) -> pd.DataFrame:
    """
    Get the other discussion items out into a parsed format.
    """
    other_items = get_other_items(text)
    other_items_df = pd.DataFrame(other_items, columns=['type', 'raw_text'])
    if not other_items:  # No other items provided
        return other_items_df
    other_items_df['name'] = other_items_df['raw_text'].str.split('\n \n').apply(lambda x: x[0]).str.replace(': ', '').str.replace('*', '').str.replace('\n', ' ')
    other_items_df.loc[other_items_df['name'].str.len() > 200, 'name'] = other_items_df['raw_text'].str.split('\n\n').apply(lambda x: x[0]).str.replace(': ', '').str.replace('*', '').str.replace('\n', ' ')
    other_items_df['dollar_amount'] = other_items_df['name'].apply(extract_dollar_amount)
    other_items_df['name'] = other_items_df.apply(lambda x: x['name'].replace(x['dollar_amount'], ''), axis=1).str.replace('-', '')
    other_items_df['status'] = other_items_df['name'].apply(extract_status)
    other_items_df['name'] = other_items_df.apply(lambda x: x['name'].replace(x['status'], ''), axis=1)
    other_items_df['name'] = other_items_df['name'].str.replace('OTHER BUSINESS', '').str.strip()
    other_items_df = other_items_df[other_items_df['name'].str.len() > 5]
    other_items_df['type'] = other_items_df['type'].str.replace('\n', '', regex=False).str.replace('  ', ' ', regex=False).str.strip()
    other_items_df['voting'] = other_items_df['raw_text'].apply(extract_voting)
    other_items_df['date'] = str(date)
    other_items_df['name'] = other_items_df['name'].str.split().apply(lambda x: ' '.join(x))
    other_items_df['ayes'] = other_items_df['voting'].apply(extract_ayes)
    other_items_df['noes'] = other_items_df['voting'].apply(extract_noes)
    other_items_df = other_items_df[other_items_df['name'].str.len() < 200]
    return other_items_df


def compile_master_dataframe(minutes_path: str):
    """
    Get a master DataFrame of all the parsed discussions
    """
    consent_dfs = []
    other_items_dfs = []
    meeting_dates = []
    meeting_texts = []

    for minutes in minutes_path:
        with open(minutes) as file:
            text = file.read()
            date = FILE_NAMES_AND_DATES[str(minutes).replace('.txt', '.pdf')]
            meeting_texts.append(text)
            meeting_dates.append(date)

            try: 
                consent_dfs.append(get_consent_agenda_df(text, date))
                other_items_dfs.append(get_other_items_df(text, date))
            except AttributeError:
                print('BAD PATH: {}'.format(str(minutes)))

    consent_combined = pd.concat(consent_dfs)
    other_items_combined = pd.concat(other_items_dfs)
    other_items_combined['discussion_type'] = 'Full discussion'
    consent_combined['discussion_type'] = 'Consent agenda'
    df = pd.concat([consent_combined, other_items_combined])

    return df, meeting_texts, meeting_dates


def get_data(req):
    print(req.text)
    errors = req.json().get("errors", None)
    if(errors is not None):
        raise Exception(errors)
    else:
        return req.json()["data"]


def get_discussions(base_url: str):
    r = requests.get(base_url + "/discussions")
    return get_data(r)


def get_issues(base_url: str):
    r = requests.get(base_url + "/issues")
    return get_data(r)


def get_meetings(base_url: str):
    r = requests.get(base_url + "/meetings")
    return get_data(r)


def get_issue_identifiers(issues):
    return set({issue["identifier"]: issue["id"] for issue in issues if issue["identifier"] is not None})


def create_issue_if_needed(issue, issues, base_url):
    if issue["identifier"] not in [i["identifier"] for i in issues]:
        r = requests.post(base_url + "/issues", json={"issue": issue})
        created_issue = get_data(r)
        print("Created issue" , issue["identifier"])
        return created_issue
    else:
        return next(x for x in issues if x["identifier"] == issue["identifier"]) 


def create_meeting_if_needed(meeting, meetings, base_url):
    if meeting["date"] not in [i["date"] for i in meetings]:
        r = requests.post(base_url + "/meetings", json={"meeting": meeting})
        created_meeting = get_data(r)
        return created_meeting
    else:
        return next(m for m in meetings if m["date"] == meeting["date"]) 


def create_discussion(discussion, discussions, base_url):
    r = requests.post(base_url + "/discussions", json={"discussion": discussion})
    return get_data(r)


def write_issues_to_database(df: pd.DataFrame, base_url: str):
    """Write issues to the database"""
    issues_df = df.copy()
    issues_df['name'] = issues_df.name.str.upper()
    issues_df = pd.DataFrame(issues_df.groupby(['name'])['date'].max()).reset_index().merge(df, on=['name', 'date'])[['name', 'status']]

    for ind, row in issues_df.iterrows():
        existing_issues = get_issues(base_url)
        issue = {
            "description": "No description",
            "identifier": row['name'] if row['name'] else 'BLANK',
            "importance": 10_000,
            "status": row['status'] if row['status'] else 'UNKNOWN',
        }
        created_issue = create_issue_if_needed(issue, existing_issues, base_url)
    
    return existing_issues


def write_meetings_to_database(df: pd.DataFrame, meeting_texts, meeting_dates, base_url):
    """Write meetings to the database"""

    # Mesh to the existing date format for meetings (YYYYY-MM-DD) from ISO
    meeting_dates = [date[0:10] for date in meeting_dates]  

    for date, text in zip(meeting_dates, meeting_texts):

        existing_meetings = get_meetings(base_url)
        meeting = {
            'date': date,
            'body': text,
        }
        if meeting['body'].strip():
            created_meeting = create_meeting_if_needed(meeting, existing_meetings, base_url)
    
    return existing_meetings


def write_discussions_to_database(df: pd.DataFrame, existing_meetings, existing_issues, base_url) -> None:
    """Write discussions to the database"""

    parsed_discussions = []
    for ind, row in df.iterrows():
        matching_issues = []
        matching_meetings = []
        try:
            matching_issues = [issue for issue in existing_issues if issue['identifier'].upper().strip() == row['name'].upper().strip()]
            matching_meetings = [meeting for meeting in existing_meetings if meeting['date'] == row['date'][:10]]
        except KeyError:  # Indicates missing issue identifier or meeting date
            pass
        if len(matching_issues) != 0 and len(matching_meetings) != 0:
            issue_id = next((issue['id'] for issue in existing_issues if issue['identifier'] == row['name'].upper()))
            meeting_id = next(meeting['id'] for meeting in existing_meetings if meeting['date'] == row['date'][:10])
        
            parsed_discussion = {
                'body': row['raw_text'],
                'issue_id': issue_id,
                'meeting_id': meeting_id,
                'votes': row['voting'],
                'discussion_type': row['discussion_type'],
                'dollar_amount': row['dollar_amount'],
                'type': row['type'],
                'status': row['status'],
            }
            if parsed_discussion['body'].strip():
                try:
                    create_discussion(parsed_discussion, [], base_url)
                except Exception:
                    print('Parsed discussion:', parsed_discussion)
                    raise Exception('Error')


if __name__ == '__main__':
    
    df, meeting_texts, meeting_dates = compile_master_dataframe(MINUTES_PATH)
    existing_issues = write_issues_to_database(df, BASE_URL)
    existing_meetings = write_meetings_to_database(df, meeting_texts, meeting_dates, BASE_URL)
    write_discussions_to_database(df, existing_issues, existing_meetings, BASE_URL)
