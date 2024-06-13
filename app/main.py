import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import psycopg2
import json

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1gK-0F3pzBV_0dLrV608dUPpjK78g3i1gXsICsRUptm8"
SAMPLE_RANGE_NAME = "Members!A2:G"

class Member:
  def __init__(self, id, last_name, first_name, city, time, place, track):
    self.id = id
    self.last_name = last_name
    self.first_name = first_name
    self.city = city
    self.time = time
    self.place = place
    self.track = track

members = []
tracks_dict = {}

def main():
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
        .execute()
    )
    values = result.get("values", [])

    if not values:
      print("No data found.")
      return

    for row in values:
      member = Member(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
      members.append(member)
      
      tracks_dict[row[6]] = None

  except HttpError as err:
    print(err)


def connect_to_db():
    config_file = open('config.json')
    config_data = json.load(config_file)

    try:
      conn = psycopg2.connect(f"dbname={config_data["pg_dbname"]} user={config_data["pg_user"]} host={config_data["pg_host"]} password={config_data["pg_password"]}")
      print("Success")

    except:
        print("I am unable to connect to the database")
        conn = None

    if conn:
        # we use a context manager to scope the cursor session
        with conn.cursor() as curs:
            for track_name in tracks_dict:
                try:
                    curs.execute("""
                                INSERT INTO tracks (name) 
                                VALUES (%s) 
                                RETURNING "id"
                                """, (track_name,))
                    
                    id = curs.fetchone()[0]
                    tracks_dict[track_name] = id

                    conn.commit()

                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

            for member in members:
                time = member.time
                if time == 'dns' or time == 'dnf':
                    time = None

                place = member.place
                if place == '':
                    place = None

                try:
                    curs.execute("""
                                INSERT INTO members (last_name, first_name, city, time, place, track_id) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """, (member.last_name, member.first_name, member.city, time, place, tracks_dict[member.track]))

                    conn.commit()

                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)



if __name__ == "__main__":
  main()
  connect_to_db()