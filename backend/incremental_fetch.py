from __future__ import print_function
import os
import json
import pickle
import base64
import email
from datetime import datetime
from whoosh.fields import Schema, TEXT, ID, DATETIME
from whoosh import index
from whoosh.qparser import QueryParser
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# We will store checkpoint in data/state.json
STATE_FILE = "data/state.json"
INDEX_DIR = "indexdir"

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
            return datetime.strptime(date_str.strip(), fmt)
        except:
            pass
    return None

def index_document(doc):
    if not index.exists_in(INDEX_DIR):
        # Re-create or handle no initial index scenario
        print("Index does not exist. Run initial indexing from MBOX first.")
        return
    ix = index.open_dir(INDEX_DIR)
    writer = ix.writer()
    writer.add_document(**doc)
    writer.commit()

def fetch_new_emails():
    state = get_state()
    last_history_id = state.get("last_history_id", None)

    service = get_gmail_service()

    # Strategy: 
    # If we have a last_history_id, we can use Gmail's history API to find changes since that history ID.
    # Otherwise, we fetch all emails (not ideal for incremental, so assume initial run done with MBOX already).
    if last_history_id:
        history = service.users().history().list(
            userId='me',
            startHistoryId=last_history_id,
            historyTypes=['messageAdded']
        ).execute()
        history_records = history.get('history', [])
        message_ids = []
        for record in history_records:
            for msg_added in record.get('messagesAdded', []):
                message_ids.append(msg_added['message']['id'])
    else:
        # If no history_id, as a fallback, fetch recent N messages
        # Better approach: Initialize last_history_id after initial MBOX indexing by searching last email date.
        # For now, let's just take recent messages (e.g. last 100)
        resp = service.users().messages().list(userId='me', maxResults=100).execute()
        message_ids = [m['id'] for m in resp.get('messages', [])]

    # Fetch and index each new message
    for mid in message_ids:
        msg_data = service.users().messages().get(userId='me', id=mid, format='raw').execute()
        raw_email = msg_data['raw']
        decoded_raw = base64.urlsafe_b64decode(raw_email)
        mime_msg = email.message_from_bytes(decoded_raw)
        # Extract fields
        message_id = mime_msg.get('Message-ID', '')
        subject = mime_msg.get('Subject', '')
        sender = mime_msg.get('From', '')
        recipients = mime_msg.get('To', '')
        date = mime_msg.get('Date', '')
        date_obj = parse_date(date) if date else None

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
            'message_id': message_id,
            'subject': subject,
            'sender': sender,
            'recipients': recipients,
            'date': date_obj,
            'body': body
        }
        index_document(doc)
        print(f"Indexed message: {message_id}")

    # Update history_id if present
    if 'historyId' in history:
        state['last_history_id'] = history['historyId']
        save_state(state)

if __name__ == "__main__":
    fetch_new_emails()
