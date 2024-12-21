import os
import json
import pickle
import base64
import email
import time
import typesense
from datetime import datetime
from email.header import decode_header
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
STATE_FILE = "data/state.json"

# Typesense Configuration
client = typesense.Client({
    'nodes': [{
        'host': 'localhost',
        'port': '8108',
        'protocol': 'http'
    }],
    'api_key': 'orion123',  # Replace with your actual Typesense API key
    'connection_timeout_seconds': 2
})

def get_state():
    """Load the last_history_id from the state file."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    """Save the last_history_id to the state file."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def get_gmail_service():
    """Authenticate and return the Gmail API service."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # Refresh or create credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                print("[ERROR] credentials.json not found. Please add your OAuth credentials.")
                exit(1)
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for future use
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('gmail', 'v1', credentials=creds)
    return service

def parse_date(date_str):
    """Parse the email date string into a Unix timestamp."""
    if not date_str:
        return int(time.time())
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return int(dt.timestamp())
        except ValueError:
            continue
    # Fallback to current time if parsing fails
    return int(time.time())

def decode_header_value(value):
    """Decode email header values into readable strings."""
    if not value:
        return ""
    decoded_parts = decode_header(value)
    decoded_str = ""
    for text, charset in decoded_parts:
        if isinstance(text, bytes):
            try:
                decoded_str += text.decode(charset or 'utf-8', errors='replace')
            except LookupError:
                decoded_str += text.decode('utf-8', errors='replace')
        else:
            decoded_str += text
    return decoded_str

def strip_html(html_content):
    """Strip HTML tags and extract text using BeautifulSoup."""
    soup = BeautifulSoup(html_content, 'html.parser')
    # Remove script and style elements
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()
    # Get text and collapse whitespace
    text = soup.get_text(separator=' ')
    text = ' '.join(text.split())
    return text

def fetch_new_emails():
    """Fetch and index new emails from Gmail into Typesense."""
    state = get_state()
    last_history_id = state.get("last_history_id", None)

    service = get_gmail_service()

    message_ids = []
    if last_history_id:
        # Use Gmail history to find new messages
        try:
            history = service.users().history().list(
                userId='me',
                startHistoryId=last_history_id,
                historyTypes=['messageAdded']
            ).execute()
        except Exception as e:
            print(f"[ERROR] Failed to fetch history: {e}")
            return

        if 'history' in history:
            for record in history['history']:
                for msg_added in record.get('messagesAdded', []):
                    message_ids.append(msg_added['message']['id'])

        # Update last_history_id
        if 'historyId' in history:
            state['last_history_id'] = history['historyId']
    else:
        # No previous history: fetch a recent set of messages
        try:
            resp = service.users().messages().list(userId='me', maxResults=100).execute()
            message_ids = [m['id'] for m in resp.get('messages', [])]
            # Update last_history_id to the latest historyId
            state['last_history_id'] = resp.get('historyId', None)
        except Exception as e:
            print(f"[ERROR] Failed to fetch messages: {e}")
            return

    if not message_ids:
        print("[INFO] No new emails to index.")
        save_state(state)
        return

    documents = []
    for mid in message_ids:
        try:
            msg_data = service.users().messages().get(userId='me', id=mid, format='raw').execute()
            raw_email = msg_data['raw']
            decoded_raw = base64.urlsafe_b64decode(raw_email)
            mime_msg = email.message_from_bytes(decoded_raw)

            # Decode headers
            message_id = decode_header_value(mime_msg.get('Message-ID', ''))
            subject = decode_header_value(mime_msg.get('Subject', 'No Subject'))
            sender = decode_header_value(mime_msg.get('From', 'Unknown Sender'))
            recipients = decode_header_value(mime_msg.get('To', ''))
            date_str = decode_header_value(mime_msg.get('Date', ''))
            date_val = parse_date(date_str)

            # Extract body
            body = ""
            if mime_msg.is_multipart():
                for part in mime_msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = part.get_content_disposition()
                    if content_disposition == 'attachment':
                        continue  # Skip attachments
                    if content_type == 'text/plain':
                        payload = part.get_payload(decode=True)
                        if payload:
                            body += payload.decode(errors="replace")
                    elif content_type == 'text/html':
                        payload = part.get_payload(decode=True)
                        if payload:
                            stripped_html = strip_html(payload.decode(errors="replace"))
                            body += stripped_html + ' '
            else:
                content_type = mime_msg.get_content_type()
                payload = mime_msg.get_payload(decode=True)
                if payload:
                    if content_type == 'text/plain':
                        body += payload.decode(errors="replace")
                    elif content_type == 'text/html':
                        body += strip_html(payload.decode(errors="replace")) + ' '

            body = body.strip()

            # Prepare document for Typesense
            doc = {
                "id": message_id,  # Set 'id' to 'message_id' for Typesense
                "subject": subject,
                "sender": sender,
                "recipients": recipients,
                "date": date_val,
                "body": body
            }
            documents.append(doc)

        except Exception as e:
            print(f"[WARN] Failed to process message ID {mid}: {e}")
            continue  # Skip to the next message

    if documents:
        try:
            response = client.collections['emails'].documents.import_(documents, {'action': 'upsert'})
            print(f"[INFO] Indexed {len(documents)} new emails.")
            print("[INFO] Typesense Import Response:")
            print(response)
        except Exception as e:
            print(f"[ERROR] Failed to import documents to Typesense: {e}")

    save_state(state)

if __name__ == "__main__":
    fetch_new_emails()
