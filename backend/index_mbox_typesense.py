import os
import mailbox
import time
import typesense
import json
from datetime import datetime
from email.header import decode_header
from bs4 import BeautifulSoup  # Import BeautifulSoup

# Configuration
API_KEY = 'orion123'
TYPESENSE_HOST = 'localhost'
TYPESENSE_PORT = '8108'
MBOX_PATH = "data/mbox_export.mbox"
CHECKPOINT_FILE = "checkpoint.json"
BATCH_SIZE = 100
LOG_INTERVAL = 1000

client = typesense.Client({
  'nodes': [{
    'host': TYPESENSE_HOST,
    'port': TYPESENSE_PORT,
    'protocol': 'http'
  }],
  'api_key': API_KEY,
  'connection_timeout_seconds': 2
})

def parse_date(date_str):
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return int(dt.timestamp())
        except:
            pass
    return int(time.time())

def decode_header_value(value):
    if not value:
        return ""
    decoded_parts = decode_header(value)
    decoded_str = ""
    for text, charset in decoded_parts:
        if charset is None:
            if isinstance(text, bytes):
                decoded_str += text.decode('utf-8', errors='replace')
            else:
                decoded_str += text
        else:
            if isinstance(text, bytes):
                try:
                    decoded_str += text.decode(charset, errors='replace')
                except LookupError:
                    decoded_str += text.decode('utf-8', errors='replace')
            else:
                decoded_str += text
    return decoded_str

def strip_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Remove script and style elements
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()
    # Get text
    text = soup.get_text(separator=' ')
    # Collapse whitespace
    text = ' '.join(text.split())
    return text

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
            return data.get("last_processed_index", 0)
    return 0

def save_checkpoint(index):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"last_processed_index": index}, f)

print(f"[INFO] Opening MBOX file at: {MBOX_PATH}")
if not os.path.exists(MBOX_PATH):
    print(f"[ERROR] MBOX file not found at {MBOX_PATH}. Exiting.")
    exit(1)

mbox = mailbox.mbox(MBOX_PATH)
message_count = len(mbox)
print(f"[INFO] Found {message_count} messages in the MBOX file.")

last_processed_index = load_checkpoint()
print(f"[INFO] Last processed index from checkpoint: {last_processed_index}")

batch = []
processed_count = last_processed_index

# Resume from last_processed_index + 1
start_index = last_processed_index + 1
print(f"[INFO] Resuming from message index {start_index}.")

for i, msg in enumerate(mbox, start=1):
    if i < start_index:
        continue  # Skip until we reach the checkpoint

    # Process message i
    message_id = decode_header_value(msg.get('Message-ID', ''))
    subject = decode_header_value(msg.get('Subject', ''))
    sender = decode_header_value(msg.get('From', ''))
    recipients = decode_header_value(msg.get('To', ''))
    date_str = decode_header_value(msg.get('Date', ''))
    date_val = parse_date(date_str) if date_str else int(time.time())

    body = ""
    try:
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = part.get_content_disposition()
                if content_type == 'text/plain' and content_disposition != 'attachment':
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode(errors="replace")
                elif content_type == 'text/html' and content_disposition != 'attachment':
                    payload = part.get_payload(decode=True)
                    if payload:
                        stripped_html = strip_html(payload.decode(errors="replace"))
                        body += stripped_html + ' '
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                content_type = msg.get_content_type()
                if content_type == 'text/plain':
                    body += payload.decode(errors="replace")
                elif content_type == 'text/html':
                    stripped_html = strip_html(payload.decode(errors="replace"))
                    body += stripped_html + ' '
    except Exception as e:
        print(f"[WARN] Error decoding body for message ID {message_id}: {e}")

    doc = {
        "message_id": message_id,
        "subject": subject,
        "sender": sender,
        "recipients": recipients,
        "date": date_val,
        "body": body.strip()
    }

    batch.append(doc)
    processed_count += 1

    if processed_count % LOG_INTERVAL == 0:
        print(f"[INFO] Processed {processed_count}/{message_count} messages. Last message ID: {message_id}, Subject: {subject}")

    # Once batch is full, import and save checkpoint
    if len(batch) >= BATCH_SIZE:
        print(f"[INFO] Importing batch of {len(batch)} documents (up to message {i}/{message_count})...")
        try:
            response = client.collections['emails'].documents.import_(batch)
            print("[INFO] Import response:")
            print(response)
            # Save checkpoint after successful batch import
            save_checkpoint(processed_count)
        except Exception as e:
            print(f"[ERROR] Failed to import batch ending at message {i}: {e}")
            # Save checkpoint so we can resume if needed
            save_checkpoint(processed_count)
            exit(1)  # exit on error if desired

        batch.clear()

# If there's a final partial batch, import it and checkpoint
if batch:
    print(f"[INFO] Importing final batch of {len(batch)} documents (total {processed_count} processed)...")
    try:
        response = client.collections['emails'].documents.import_(batch)
        print("[INFO] Final batch import response:")
        print(response)
        save_checkpoint(processed_count)
    except Exception as e:
        print(f"[ERROR] Failed to import final batch: {e}")
        save_checkpoint(processed_count)
        exit(1)
    batch.clear()

print("[INFO] Incremental indexing from MBOX complete.")
print(f"[INFO] Total messages processed: {processed_count}")
