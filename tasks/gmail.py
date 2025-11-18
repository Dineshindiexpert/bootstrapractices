import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import pandas as pd
import time
import tqdm

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    # Get API Credential
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('gmail', 'v1', credentials=creds)

    # List All Messagges
    consumed_token = 0
    db = []
    print("Get mail id list")
    if os.path.exists('messages.json'):
        df = pd.read_json('messages.json')
    else:
        try:
            maxResults = 500
            request = service.users().messages().list(userId='me', labelIds=['CATEGORY_PROMOTIONS'], maxResults=maxResults)
            while request is not None:
                results = request.execute()
                consumed_token += 5
                messages = results.get('messages', [])
                db += messages
                request = service.users().messages().list_next(request, results)
        finally:
            df = pd.DataFrame(db)
            df['from'] = pd.Series(dtype='string')
            df['subject'] = pd.Series(dtype='string')
            df['timestamp'] = pd.Series(dtype='string')

            df.to_json('messages.json', orient='records')

    print("Got mail id List: {} messages".format(len(df.index)))
    print("Consumed Token {}".format(consumed_token))

    # Get Mail Headers [From, Sbuject, Date]
    if len(df.index) < 0:
        print('No message found.')
    else:
        try: 
            for i in tqdm.tqdm(df.index):
                if not pd.isnull(df['from'][i]):
                    # Skip fetched data
                    continue
                result = service.users().messages().get(userId='me', id=df['id'][i], format='metadata', 
                                                        metadataHeaders=['From','Subject','Date']).execute()
                consumed_token += 5
                body = result.get('payload',[])

                if not body:
                    print("Token Limit per minutes may be reached")
                    break

                for el in body['headers']:
                    if el['name'] == 'From':
                        df.loc[i, 'from'] = el['value']
                    elif el['name'] == 'Subject':
                        df.loc[i,'subject'] = el['value']
                    elif el['name'] == 'Date':
                        df.loc[i, 'timestamp'] = pd.Timestamp(el['value'])
                time.sleep(1/40)
        finally:
            df.to_json('messages.json', orient='records')
            print("Consumed Token {}".format(consumed_token))

if __name__ == '__main__':
    main()
 