from pathlib import Path

import pandas as pd

from scripts.parse import compile_master_dataframe
from scripts.persist import create_discussion, get_issues, get_meetings, create_meeting_if_needed, \
    create_issue_if_needed

BASE_URL = "https://thoughtful-confused-italiangreyhound.gigalixirapp.com/api"
MINUTES_PATH = Path('../data/cville_pdfs/minutes/').glob('*.txt')

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
    # existing_issues = write_issues_to_database(df, BASE_URL)
    # existing_meetings = write_meetings_to_database(df, meeting_texts, meeting_dates, BASE_URL)
    # existing_issues = get_issues(BASE_URL)
    # existing_meetings = get_meetings(BASE_URL)
    # write_discussions_to_database(df, existing_meetings, existing_issues, BASE_URL)
