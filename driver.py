from pathlib import Path

import pandas as pd

from parse import compile_master_dataframe
from persist import create_discussion, get_issues, get_meetings, create_meeting_if_needed, \
    create_issue_if_needed

# BASE_URL = "https://thoughtful-confused-italiangreyhound.gigalixirapp.com/api"
BASE_URL = "http://localhost:4000/api"
MINUTES_PATH = Path('./data/cville_pdfs/minutes/').glob('*.txt')

def get_issue_pairs(df):
    pairs = []
    for name in df["name"].unique():
        statuses = df[df["name"] == name]["status"]
        statuses = statuses[statuses != ""].tail(1)
        if name is not "":
            if len(statuses) > 0:
                pairs.append((name, statuses.iloc[0]))
            else:
                pairs.append((name, "UNKNOWN"))
    return pairs

def write_issues_to_database(df: pd.DataFrame, base_url: str):
    """Write issues to the database"""
    pairs = get_issue_pairs(df)

    existing_issues = get_issues(base_url)
    for name, status in pairs:
        issue = {
            "description": "No description",
            "identifier": name,
            "importance": 0,
            "status": status if status is not None else "UNKNOWN",
        }
        created_issue = create_issue_if_needed(issue, existing_issues, base_url)
        if(created_issue):
            print("Created", name)

    return get_issues(base_url)


def write_meetings_to_database(df: pd.DataFrame, meeting_texts, meeting_dates, base_url):
    """Write meetings to the database"""

    # Mesh to the existing date format for meetings (YYYYY-MM-DD) from ISO
    meeting_dates = [date[0:10] for date in meeting_dates if date is not None]
    meetings_to_create = dict((date, (date, text)) for (date, text) in zip(meeting_dates, meeting_texts)).values()

    existing_meetings = get_meetings(base_url)
    for date, text in meetings_to_create:
        meeting = {
            'date': date,
            'body': text,
        }
        if meeting['body'].strip():
            created_meeting = create_meeting_if_needed(meeting, existing_meetings, base_url)
    
    return get_meetings(base_url)


def write_discussions_to_database(df: pd.DataFrame, existing_meetings, existing_issues, base_url) -> None:
    """Write discussions to the database"""

    parsed_discussions = []
    for ind, discussion in df.iterrows():
        matching_issues = []
        matching_meetings = []
        try:
            matching_issues = [issue for issue in existing_issues if issue['identifier'].upper().strip() == discussion['name'].upper().strip()]
            matching_meetings = [meeting for meeting in existing_meetings if meeting['date'] == discussion['date'][:10]]
            if(len(matching_issues) == 0):
                print("No match", discussion["name"], discussion["date"])
            if(len(matching_issues) == 2):
                print(matching_issues)
                print(discussion)
        except KeyError:  # Indicates missing issue identifier or meeting date
            pass
        if len(matching_issues) != 0 and len(matching_meetings) != 0:
            issue_id = next(iter(matching_issues))["id"]
            meeting_id = next(iter(matching_meetings))["id"]

            parsed_discussion = {
                'body': discussion['raw_text'],
                'issue_id': issue_id,
                'meeting_id': meeting_id,
                'votes': discussion['voting'],
                'discussion_type': discussion['discussion_type'],
                'dollar_amount': discussion['dollar_amount'],
                'type': discussion['type'],
                'status': discussion['status'],
            }
            if parsed_discussion['body'].strip():
                try:
                    create_discussion(parsed_discussion, [], base_url)
                except Exception:
                    print('Error. Parsed discussion:', parsed_discussion)
                    raise Exception('Error')


if __name__ == '__main__':
    df, meeting_texts, meeting_dates = compile_master_dataframe(MINUTES_PATH)
    existing_issues = write_issues_to_database(df, BASE_URL)
    # existing_issues = get_issues(BASE_URL)
    existing_meetings = write_meetings_to_database(df, meeting_texts, meeting_dates, BASE_URL)
    # existing_meetings = get_meetings(BASE_URL)
    write_discussions_to_database(df, existing_meetings, existing_issues, BASE_URL)
