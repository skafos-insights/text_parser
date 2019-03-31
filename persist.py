import requests

def get_data(req):
    try:
        errors = req.json().get("errors", None)
        if(errors is not None):
            raise Exception(errors)
        else:
            return req.json()["data"]
    except:
        print("Error:")
        print(req.text)


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
        print("Created meeting" , meeting["date"])
        return created_meeting
    else:
        return next(m for m in meetings if m["date"] == meeting["date"])
import json
def update_meeting(meeting, base_url):
    r = requests.put(base_url + "/meetings/" + str(meeting['id']), json={"meeting": meeting})
    if r.status_code != 200:
        print("Failed to update meeting!" , meeting["date"], json.dumps({"meeting": meeting}))
    updated_meeting = get_data(r)
    return updated_meeting


def create_discussion(discussion, discussions, base_url):
    if discussion["body"] not in [d["body"] for d in discussions]:
        r = requests.post(base_url + "/discussions", json={"discussion": discussion})
        created_discussion = get_data(r)
        if created_discussion is not None:
            print("Created discussion" , discussion["body"][0:50].replace("\n", ""))
        return created_discussion
    else:
        return next(d for d in discussions if d["body"] == discussion["body"])
