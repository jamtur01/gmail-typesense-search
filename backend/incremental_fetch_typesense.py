import os
import json
import pickle
import base64
import email
import time
import typesense
from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
STATE_FILE = "data/state.json"

client = typesense.Client({
  'nodes': [{
    'host': 'localhost',
    'port': '8108',
    'protocol': 'http'
  }],
  'api_key': 'orion123',
  'connection_timeout_seconds': 2
})

def get_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def get_gmail_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    return service

def parse_date(date_str):
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return int(dt.timestamp())
        except:
            pass
    return int(time.time())

def fetch_new_emails():
    state = get_state()
    last_history_id = state.get("last_history_id", None)

    service = get_gmail_service()

    message_ids = []
    if last_history_id:
        # Use Gmail history to find new messages
        history = service.users().history().list(
            userId='me',
            startHistoryId=last_history_id,
            historyTypes=['messageAdded']
        ).execute()

        if 'history' in history:
            for record in history['history']:
                for msg_added in record.get('messagesAdded', []):
                    message_ids.append(msg_added['message']['id'])
        # Update last_history_id
        if 'historyId' in history:
            state['last_history_id'] = history['historyId']
    else:
        # No previous history: fetch a recent set of messages
        resp = service.users().messages().list(userId='me', maxResults=100).execute()
        message_ids = [m['id'] for m in resp.get('messages', [])]
        # After first run, consider setting last_history_id from a known email.

    documents = []
    for mid in message_ids:
        msg_data = service.users().messages().get(userId='me', id=mid, format='raw').execute()
        raw_email = msg_data['raw']
        decoded_raw = base64.urlsafe_b64decode(raw_email)
        mime_msg = email.message_from_bytes(decoded_raw)

        message_id = mime_msg.get('Message-ID', '')
        subject = mime_msg.get('Subject', '')
        sender = mime_msg.get('From', '')
        recipients = mime_msg.get('To', '')
        date_str = mime_msg.get('Date', '')
        date_val = parse_date(date_str) if date_str else int(time.time())

        body = ""
        if mime_msg.is_multipart():
            for part in mime_msg.walk():
                if part.get_content_type() == 'text/plain':
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode(errors="replace")
        else:
            payload = mime_msg.get_payload(decode=True)
            if payload:
                body = payload.decode(errors="replace")

        doc = {
            "message_id": message_id,
            "subject": subject,
            "sender": sender,
            "recipients": recipients,
            "date": date_val,
            "body": body
        }
        documents.append(doc)

    if documents:
        response = client.collections['emails'].documents.import_(documents)
        print(response)

    save_state(state)

if __name__ == "__main__":
    fetch_new_emails()
