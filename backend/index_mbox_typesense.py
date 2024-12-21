import os
import json
import pickle
import base64
import email
import time
import typesense
from datetime import datetime
from email.header import decode_header
from bs4 import BeautifulSoup  # Import BeautifulSoup
import logging  # Import logging module
from textblob import TextBlob  # For sentiment analysis
from openai import OpenAI

client = OpenAI(api_key=OPENAI_API_KEY)  # For embedding generation
from logging.handlers import RotatingFileHandler  # For log rotation
import sys  # For exiting the script
from dotenv import load_dotenv
import mailbox

# Load environment variables from .env file (Recommendation 1)
load_dotenv()

# Configuration
API_KEY = os.getenv('TYPESENSE_API_KEY', 'orion123')  # Replace with your actual Typesense API key or set as environment variable
TYPESENSE_HOST = 'localhost'
TYPESENSE_PORT = '8108'
MBOX_PATH = "../data/mbox_export.mbox"  # Adjusted path based on user output
CHECKPOINT_FILE = "checkpoint.json"
BATCH_SIZE = 1000  # Increased batch size for efficiency
LOG_INTERVAL = 10000  # Increased log interval for large files
LOG_FILE = "indexing.log"  # Log file path
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', 'your_openai_api_key_here')  # Replace with your actual OpenAI API key or set as environment variable

# Setup Logging (Recommendation 1)
logger = logging.getLogger('EmailIndexer')
logger.setLevel(logging.INFO)

# Create a rotating file handler
handler = RotatingFileHandler(LOG_FILE, maxBytes=10**7, backupCount=5)  # 10MB per file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize Typesense Client
client = typesense.Client({
    'nodes': [{
        'host': TYPESENSE_HOST,
        'port': TYPESENSE_PORT,
        'protocol': 'http'
    }],
    'api_key': API_KEY,
    'connection_timeout_seconds': 2
})

# Initialize OpenAI API

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

def load_checkpoint():
    """Load the last_processed_index from the checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            try:
                data = json.load(f)
                return data.get("last_processed_index", 0)
            except json.JSONDecodeError:
                logger.warning(f"{CHECKPOINT_FILE} is empty or malformed. Initializing to 0.")
                return 0
    else:
        logger.info(f"{CHECKPOINT_FILE} does not exist. Starting from 0.")
        return 0

def save_checkpoint(index):
    """Save the last_processed_index to the checkpoint file."""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_processed_index": index}, f)

def validate_message_id(message_id):
    """Validate that the message_id is not empty and follows a standard format."""
    if not message_id:
        logger.warning("Email without message_id found. Skipping indexing.")
        return False
    if not isinstance(message_id, str):
        logger.warning(f"Invalid message_id type: {type(message_id)}. Skipping indexing.")
        return False
    if not message_id.startswith("<") or not message_id.endswith(">"):
        logger.warning(f"Malformed message_id: {message_id}. Skipping indexing.")
        return False
    return True

def extract_labels(msg):
    """Extract labels from the email message."""
    labels = msg.get('X-Gmail-Labels', '')
    if labels:
        labels = decode_header_value(labels)
    else:
        labels = []
    # Assuming labels are comma-separated if present
    if isinstance(labels, str):
        labels = [label.strip() for label in labels.split(',')]
    return labels

def extract_thread_id(msg):
    """Extract thread ID from the email message."""
    thread_id = msg.get('Thread-Id', '')
    if thread_id:
        thread_id = decode_header_value(thread_id)
    else:
        thread_id = ''
    return thread_id

def analyze_sentiment(text):
    """Analyze the sentiment of the given text using TextBlob."""
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity  # Range from -1 (negative) to 1 (positive)
    if sentiment > 0.1:
        return "positive"
    elif sentiment < -0.1:
        return "negative"
    else:
        return "neutral"

def generate_embedding(text):
    """Generate an embedding vector for the given text using OpenAI's API."""
    try:
        response = client.embeddings.create(input=text,
        model="text-embedding-ada-002")
        embedding = response.data[0].embedding
        return embedding
    except Exception as e:
        logger.error(f"Failed to generate embedding: {e}")
        return []

def import_documents_with_retries(batch, max_retries=5):
    """
    Import documents to Typesense with a retry mechanism.
    Implements exponential backoff.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = client.collections['emails'].documents.import_(batch, {'action': 'upsert'})
            logger.info(f"Successfully indexed batch of {len(batch)} emails.")
            return response
        except Exception as e:
            attempt += 1
            wait_time = 2 ** attempt  # Exponential backoff
            logger.error(f"Attempt {attempt} - Failed to import batch: {e}. Retrying in {wait_time} seconds.")
            time.sleep(wait_time)
    logger.critical(f"Failed to import batch after {max_retries} attempts. Exiting.")
    sys.exit(1)

def index_emails():
    """Main function to index emails from the mbox file into Typesense."""
    logger.info(f"Opening MBOX file at: {MBOX_PATH}")
    if not os.path.exists(MBOX_PATH):
        logger.error(f"MBOX file not found at {MBOX_PATH}. Exiting.")
        sys.exit(1)

    try:
        mbox = mailbox.mbox(MBOX_PATH)
    except Exception as e:
        logger.error(f"Failed to open MBOX file: {e}. Exiting.")
        sys.exit(1)

    message_count = len(mbox)
    logger.info(f"Found {message_count} messages in the MBOX file.")

    last_processed_index = load_checkpoint()
    logger.info(f"Last processed index from checkpoint: {last_processed_index}")

    batch = []
    processed_count = last_processed_index

    # Resume from last_processed_index + 1
    start_index = last_processed_index + 1
    logger.info(f"Resuming from message index {start_index}.")

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

        # Validate message_id (Recommendation 3.2)
        if not validate_message_id(message_id):
            continue  # Skip indexing if message_id is invalid

        # Extract additional metadata (Recommendation 6)
        labels = extract_labels(msg)
        thread_id = extract_thread_id(msg)

        body = ""
        try:
            if msg.is_multipart():
                for part in msg.walk():
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
                content_type = msg.get_content_type()
                payload = msg.get_payload(decode=True)
                if payload:
                    if content_type == 'text/plain':
                        body += payload.decode(errors="replace")
                    elif content_type == 'text/html':
                        body += strip_html(payload.decode(errors="replace")) + ' '
        except Exception as e:
            logger.warning(f"Error decoding body for message ID {message_id}: {e}")
            continue  # Skip to the next message

        body = body.strip()

        # Perform sentiment analysis (Recommendation 6)
        sentiment = analyze_sentiment(body)

        # Generate embedding vector (Recommendation 6)
        body_vector = generate_embedding(body)

        # Prepare document for Typesense
        doc = {
            "id": message_id,  # Set 'id' to 'message_id' for Typesense
            "subject": subject,
            "sender": sender,
            "recipients": recipients,
            "date": date_val,
            "body": body,
            "labels": labels,        # Additional metadata
            "thread_id": thread_id,  # Additional metadata
            "sentiment": sentiment,  # Enriched data
            "body_vector": body_vector  # Embedding vector
        }

        batch.append(doc)
        processed_count += 1

        # Logging progress (Recommendation 3.3)
        if processed_count % LOG_INTERVAL == 0:
            logger.info(f"Processed {processed_count}/{message_count} messages. Last message ID: {message_id}, Subject: {subject}")

        # Once batch is full, import and save checkpoint
        if len(batch) >= BATCH_SIZE:
            logger.info(f"Importing batch of {len(batch)} documents (up to message {i}/{message_count})...")
            import_documents_with_retries(batch)
            # Save checkpoint after successful batch import
            save_checkpoint(processed_count)
            batch.clear()

    # If there's a final partial batch, import it and checkpoint
    if batch:
        logger.info(f"Importing final batch of {len(batch)} documents (total {processed_count} processed)...")
        import_documents_with_retries(batch)
        # Save checkpoint after successful batch import
        save_checkpoint(processed_count)
        batch.clear()

    logger.info("Manual indexing from MBOX complete.")
    logger.info(f"Total messages processed: {processed_count}")

if __name__ == "__main__":
    index_emails()
