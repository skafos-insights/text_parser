from pathlib import Path

import pandas as pd
import math
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json
import re

from parse import compile_master_dataframe
from persist import create_discussion, get_issues, get_meetings, create_meeting_if_needed, \
    create_issue_if_needed, update_meeting

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


def get_granicus_meetings(base_url):
    server_meetings = pd.DataFrame(get_meetings(base_url))
    server_meetings = server_meetings.drop(columns=['body']) #drop body to make reading jsons easier, not using it
    server_meetings['date'] = pd.to_datetime(server_meetings['date'])
    
    df = pd.read_json("granicus-scrape/meetings.json")
    df['date'] = pd.to_datetime(df['date'])
    # print(server_meetings)
    print(df.columns.values)
    print(server_meetings.columns.values)
    no_match = []
    double_match = []
    no_item_match = []
    almost_match = []
    
    server_meetings['title'] = None
    server_meetings['granicus_clip_id'] = None
    server_meetings['media_player'] = None
    server_meetings['agenda_html_url'] = None
    server_meetings['granicus_duration'] = None
    server_meetings['granicus_mp4'] = None
    server_meetings['video_file'] = None
    
    for index, row in df.iterrows():
        server_meeting = server_meetings.loc[server_meetings['date'] == row.date]
        if server_meeting.empty: no_match.append(row.date.strftime('%m/%d/%Y')); continue
        if server_meeting.shape[0] > 1: double_match.append(row.date.strftime('%m/%d/%Y'))
        print(row.date)
        #print(match, match.shape[0])
        # print(row)
        # print(json.dumps(row.tolist()))
        # print(row['agenda_viewer'])
        # print((row['agenda_viewer']))
        # print(type(row['agenda_viewer']))
        # print(json.dumps(row['agenda_viewer']))
        # print(isinstance(row.agenda_viewer, str))
        # print(row['clip_id'])
        # print(int(row['clip_id']))
        # server_meeting = match#.iloc[0:0]
        server_meetings.at[server_meeting.index[0],'title'] = row['name'] if 'name' in row else None
        server_meetings.at[server_meeting.index[0],'granicus_clip_id'] = int(row['clip_id']) if 'clip_id' in row else None
        server_meetings.at[server_meeting.index[0],'media_player'] = row['media_player'] if 'media_player' in row else None
        server_meetings.at[server_meeting.index[0],'agenda_html_url'] = row['agenda_viewer'] if 'agenda_viewer' in row and isinstance(row.agenda_viewer, str) else None
        server_meetings.at[server_meeting.index[0],'granicus_duration'] = row['duration'] if 'duration' in row else None
        server_meetings.at[server_meeting.index[0],'granicus_mp4'] = row['video'] if 'video' in row else None
        server_meetings.at[server_meeting.index[0],'video_file'] = str(row['video_file']) if 'video_file' in row and isinstance(row.agenda_viewer, str) else None
        # print(server_meetings.at[server_meeting.index[0],'granicus_clip_id'])
        # server_meeting['granicus_clip_id'] = row['clip_id']
        # server_meeting['media_player'] = row['media_player']
        # server_meeting['agenda_html_url'] = row['agenda_viewer']
        # server_meeting['granicus_duration'] = row['duration']
        # server_meeting['granicus_mp4'] = row['video']
        # server_meeting['video_file'] = row['video_file']
        # print(server_meetings.iloc[server_meeting.index[0]])
        # print(server_meetings.at[server_meeting.index[0],'granicus_clip_id'])
        #print('server_meeting' ,server_meeting['granicus_clip_id'])
        #print('match', match['granicus_clip_id'])
        # print('row', row['clip_id'])
        # server_meetings.update(server_meeting)
        # print(server_meetings.at[server_meeting.index[0],'granicus_clip_id'])
        #match = server_meetings[server_meetings['date'] == row.date]
        #server_meeting = match.iloc[0:0]
        #print(server_meeting['granicus_clip_id'])
        #print(server_meeting['granicus_clip_id'].to_json(orient='records', lines=True))
        
        if isinstance(row['agenda'], list):
            agenda = pd.DataFrame(row['agenda'])
            
            server_agenda = server_meeting.iloc[0]['discussions']
            server_agenda_names = [d['issue']['identifier'] for d in server_meeting.iloc[0]['discussions'] if 'issue' in d]
            # print("server_agenda_names", server_agenda_names)
            # print("server_agenda", server_agenda.columns.values)
            #print("agenda", agenda.columns.values)
            for i, agenda_item in agenda.iterrows():
                strip_preface = agenda_item.item_name;
                strip_preface = re.sub(r'^[a-z1-9]\.\s*','',strip_preface);
                strip_preface = re.sub(r'^(?:RESOLUTION|PUBLIC HEARING|ORDINANCE|REPORT|[\*\/\s:])+\s+','',strip_preface, re.IGNORECASE)
                # strip_preface = re.sub(r'^[A-Z\*/\s]+:\s+','',strip_preface)
                item_match, score = process.extractOne(strip_preface,server_agenda_names, scorer=fuzz.token_set_ratio)
                
                if not item_match: no_item_match.append(strip_preface+": "+row.date.strftime('%m/%d/%Y')); continue
                if score > 70:
                    #print('match',str(score))
                    #print('match',item_match)
                    server_item = next(x for x in server_agenda if x['issue']['identifier'] == item_match)
                    #print(server_item)
                    server_item['duration'] = int(agenda_item.duration) if 'duration' in agenda_item and not math.isnan(agenda_item.duration) else None
                    if not server_item['duration'] is None and math.isnan(server_item['duration']):
                        print('duration nan!', server_item['duration'], type(agenda_item.duration), agenda_item)
                        return
                    server_item['timestamp'] = int(agenda_item.timestamp) if 'timestamp' in agenda_item and not math.isnan(agenda_item.timestamp) else None
                    server_item['video_file'] = agenda_item.video_file if 'video_file' in agenda_item else None
                    
                    #print(server_item)
                elif strip_preface not in ["CALL TO ORDER","ROLL CALL","MATTERS BY THE PUBLIC","CLOSED MEETING","OTHER BUSINESS","ANNOUNCEMENTS","PROCLAMATIONS","BREAK","ANNOUNCEMENTS/PROCLAMATIONS","ANNOUNCEMENTS / PROCLAMATIONS","COMMUNITY MATTERS","CONSENT AGENDA","CONSENT AGENDA:", "CONSENT AGENDA* (Items removed from consent agenda will be considered at the end of the regular agenda)","CITY MANAGER RESPONSE TO COMMUNITY MATTERS (FROM PREVIOUS MEETINGS)", "CITY MANAGER RESPONSE TO COMMUNITY MATTERS","AWARDS/RECOGNITIONS & ANNOUNCEMENTS","AWARDS/RECOGNITIONS/ANNOUNCEMENTS","CITY MANAGER RESPONSE TO MATTERS BY THE PUBLIC","COUNCIL RESPONSE TO MATTERS BY THE PUBLIC","COUNCIL RESPONSE"]:
                    no_item_match.append({
                        "name": strip_preface,
                        "server_name": item_match,
                        "meeting_date": row.date.strftime('%m/%d/%Y'),
                        "minutes_url": (server_meeting.iloc[0].minutes_url if 'minutes_url' in server_meeting.iloc[0] else None),
                        "score": score
                    }); 
                    if score > 60:
                        almost_match.append({
                            "name": strip_preface,
                            "server_name": item_match,
                            "meeting_date": row.date.strftime('%m/%d/%Y'),
                            "score": score
                        }); 
                    
                #print(score)
                # print(agenda_item.item_name)
                #print(strip_preface)
                #print(item_match)
    server_meetings['date'] = server_meetings['date'].dt.strftime("%Y-%m-%d")
    print(server_meetings.shape[0])
    
    print(server_meetings[server_meetings.granicus_clip_id.notna()].iloc[0:1])
    print(server_meetings[server_meetings.granicus_clip_id.notna()]['granicus_clip_id'].to_json())
    #print(server_meetings[server_meetings.granicus_clip_id.notna()].iloc[0:1].to_json(orient='records'))
    print(server_meetings[server_meetings.granicus_clip_id.notna()].iloc[0:1]['granicus_clip_id'][0])
    print(type(server_meetings[server_meetings.granicus_clip_id.notna()].iloc[0:1]['granicus_clip_id'][0]))
    # server_meetings['granicus_clip_id'] = server_meetings['granicus_clip_id'].notna().astype(int)
    # print(server_meetings[server_meetings.granicus_clip_id.notna()].granicus_clip_id.to_json())
    # df = df[df.date == ]
    
    print("no_match ",len(no_match))
    # print(json.dumps(no_match))
    print("double_match ", len(double_match))
    # print(json.dumps(double_match))
    # print("no_item_match")
    # print(json.dumps(no_item_match))
    print("almost_match ",len(almost_match))
    # print(json.dumps(almost_match))
    print("no_match")
    print(json.dumps(no_match))
    
    # for index, row in server_meetings.iterrows():
        # print(server_meetings.columns.values)
        # if row["granicus_clip_id"]:
            # print(row.to_json())
            # print(row["granicus_clip_id"])
    # server_meetings['agenda_html_url'] = server_meetings['agenda_html_url'].fillna("")
    server_meetings['granicus_clip_id'] = server_meetings['granicus_clip_id'].fillna(0)#.replace({pd.np.nan: 0}) # Using None causes pandas to cast to float (castable to numpy.Int64), which breaks phoenix
    print(server_meetings[server_meetings.granicus_clip_id.notna()]['granicus_clip_id'].to_json())
    # server_meetings['granicus_clip_id'] = server_meetings['granicus_clip_id'].astype(int)
    # print(server_meetings[server_meetings.granicus_clip_id.notna()]['granicus_clip_id'].to_json())
    # rows = server_meetings.to_dict(orient='records')
    rows = [{col:getattr(row, col) for col in server_meetings} for row in server_meetings.itertuples()]

    #print(rows)
    for i in range(len(rows)):
        row = rows[i]
        # print(json.dumps(row))
        if row["granicus_clip_id"] is not None:
            update_meeting(row, base_url)
            # print(json.dumps(row))
    server_meetings[server_meetings['granicus_clip_id'].notnull()].to_json('test.json',orient='records')
    # print("server_meetings")
    # print(json.dumps(server_meetings.values.tolist()))

if __name__ == '__main__':
    df, meeting_texts, meeting_dates = compile_master_dataframe(MINUTES_PATH)
    existing_issues = write_issues_to_database(df, BASE_URL)
    # existing_issues = get_issues(BASE_URL)
    existing_meetings = write_meetings_to_database(df, meeting_texts, meeting_dates, BASE_URL)
    # existing_meetings = get_meetings(BASE_URL)
    write_discussions_to_database(df, existing_meetings, existing_issues, BASE_URL)
    get_granicus_meetings(BASE_URL);
	
