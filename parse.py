import pandas as pd
import itertools
from itertools import zip_longest
import re
from typing import List, Tuple

from constants import FILE_NAMES_AND_DATES


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
    pat = re.compile(r'\$\d+\,*\d*\.*\d*', re.DOTALL)
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    else:
        return ''


def extract_status(text: str) -> str:
    pat = re.compile(r'\([a-z]+.*\)', re.DOTALL)
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    pat = re.compile(r'\(\d+[a-z].*\)', re.DOTALL)
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]
    else:
        return ''


def extract_voting(text: str) -> str:
    pat = re.compile(r'\(Ayes.*\)', re.DOTALL)
    matches = pat.findall(str(text).replace('\n', ''))
    if matches:
        return matches[0]
    return ''


def extract_all_caps(text: str) -> str:
    pat = re.compile(r'(([A-Z]|\s)+)', re.DOTALL)
    if pat.findall(str(text)):
        return pat.findall(str(text))[0]


def extract_ayes(text: str) -> str:
    pat = re.compile(r'\(Ayes:.*;', re.DOTALL)
    if pat.findall(str(text)):
        return pat.findall(str(text))[0].split(': ')[1]
    return ''


def extract_noes(text: str) -> str:
    pat = re.compile(r'Noes:.*\.', re.DOTALL)
    matches = pat.findall(str(text))
    if matches:
        split = matches[0].split(': ')
        if len(split) > 1:
            return split[1]
    return ""


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
        consent_df['name'] = consent_df['name'].str.replace('\n', '').str.split(': ').apply(lambda x: x[1:]).apply(
            lambda x: ' '.join(x))
        consent_df['dollar_amount'] = consent_df['name'].apply(extract_dollar_amount)
        consent_df.loc[consent_df['dollar_amount'] == '', 'dollar_amount'] = consent_df['raw_text'].apply(
            extract_dollar_amount)
        consent_df['name'] = consent_df.apply(lambda x: x['name'].replace(x['dollar_amount'], ''), axis=1).str.replace(
            '-', '')
        consent_df['status'] = consent_df['raw_text'].apply(extract_status)
        consent_df['name'] = consent_df.apply(lambda x: x['name'].replace(x['status'], ''), axis=1)
        consent_df['name'] = consent_df['name'].str.replace('(2nd', '', regex=False).str.replace('(carried)', '',
                                                                                                 regex=False).str.replace(
            '(CARRIED)', '', regex=False).str.replace('(Carried)', '', regex=False).str.strip()
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
    other_items_df['name'] = other_items_df['raw_text'].str.split('\n \n').apply(lambda x: x[0]).str.replace(': ',
                                                                                                             '').str.replace(
        '*', '').str.replace('\n', ' ')
    other_items_df.loc[other_items_df['name'].str.len() > 200, 'name'] = other_items_df['raw_text'].str.split(
        '\n\n').apply(lambda x: x[0]).str.replace(': ', '').str.replace('*', '').str.replace('\n', ' ')
    other_items_df['dollar_amount'] = other_items_df['name'].apply(extract_dollar_amount)
    other_items_df['name'] = other_items_df.apply(lambda x: x['name'].replace(x['dollar_amount'], ''),
                                                  axis=1).str.replace('-', '')
    other_items_df['status'] = other_items_df['name'].apply(extract_status)
    other_items_df['name'] = other_items_df.apply(lambda x: x['name'].replace(x['status'], ''), axis=1)
    other_items_df['name'] = other_items_df['name'].str.replace('OTHER BUSINESS', '').str.strip()
    other_items_df = other_items_df[other_items_df['name'].str.len() > 5]
    other_items_df['type'] = other_items_df['type'].str.replace('\n', '', regex=False).str.replace('  ', ' ',
                                                                                                   regex=False).str.strip()
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
            print(str(minutes))
            minutes_id = str(minutes).split("/")[-1].split(".")[0]
            meeting_texts.append(text)
            meeting_dates.append(date)

            try:
                consent_agenda_df = get_consent_agenda_df(text, date)
                consent_agenda_df["document_id"] = minutes_id
                consent_dfs.append(consent_agenda_df)
                other_items_df = get_other_items_df(text, date)
                other_items_df["document_id"] = minutes_id
                other_items_dfs.append(other_items_df)
            except AttributeError:
                print('BAD PATH: {}'.format(str(minutes)))

    consent_combined = pd.concat(consent_dfs)
    other_items_combined = pd.concat(other_items_dfs)
    other_items_combined['discussion_type'] = 'Full discussion'
    consent_combined['discussion_type'] = 'Consent agenda'
    df = pd.concat([consent_combined, other_items_combined])

    return df, meeting_texts, meeting_dates
