import os
import mailbox
from whoosh.fields import Schema, TEXT, ID, DATETIME
from whoosh import index
from datetime import datetime

# Define the schema
schema = Schema(
    message_id=ID(stored=True, unique=True),
    subject=TEXT(stored=True),
    sender=TEXT(stored=True),
    recipients=TEXT(stored=True),
    date=DATETIME(stored=True),
    body=TEXT
)

def create_index(index_dir="indexdir"):
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
    return index.create_in(index_dir, schema)

def parse_date(date_str):
    # Attempt to parse the date
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except:
            pass
    return None

def index_mbox(mbox_path="data/mbox_export.mbox", index_dir="indexdir"):
    ix = create_index(index_dir)
    writer = ix.writer()
    
    mbox = mailbox.mbox(mbox_path)
    for msg in mbox:
        message_id = msg.get('Message-ID', '')
        subject = msg.get('Subject', '')
        sender = msg.get('From', '')
        recipients = msg.get('To', '')
        date = msg.get('Date', '')
        date_obj = parse_date(date) if date else None

        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == 'text/plain':
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode(errors="replace")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode(errors="replace")

        writer.add_document(
            message_id=message_id,
            subject=subject,
            sender=sender,
            recipients=recipients,
            date=date_obj,
            body=body
        )

    writer.commit()
    print("Initial indexing from MBOX complete.")

if __name__ == "__main__":
    index_mbox()

