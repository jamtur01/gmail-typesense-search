import os
import gc
import json
import time
import typesense
import mailbox
import sys
import uuid
import logging
from datetime import datetime
from email.header import decode_header
from bs4 import BeautifulSoup
from textblob import TextBlob
from openai import OpenAI
from sentence_transformers import SentenceTransformer
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# Additional imports for enrichments
import spacy
from rake_nltk import Rake
from langdetect import detect, LangDetectException
import pandas as pd
from transformers import pipeline, AutoTokenizer
from concurrent.futures import ThreadPoolExecutor, as_completed
import shelve
import torch
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable MPS globally
os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'

def initialize_local_embedding_model():
    """Initialize the embedding model with memory management."""
    try:
        # Clear any existing cached data
        gc.collect()
        torch.cuda.empty_cache() if torch.cuda.is_available() else None
        
        model = SentenceTransformer('all-MiniLM-L6-v2')
        print("Local embedding model loaded successfully.")
        return model
    except Exception as e:
        print(f"Error loading local embedding model: {e}")
        sys.exit(1)

def initialize_pipelines():
    """Initialize ML pipelines with safer device handling."""
    device = -1  # Always use CPU
    print(f"Using device: CPU (forced to avoid memory issues)")
    
    try:
        gc.collect()
        torch.cuda.empty_cache() if torch.cuda.is_available() else None
        
        summarizer = pipeline(
            "summarization",
            model="pszemraj/long-t5-tglobal-base-16384-book-summary",
            device=device,
            batch_size=1,
            max_length=512,
            framework="pt"
        )
        print("Summarization pipeline loaded successfully.")
        return summarizer
        
    except Exception as e:
        print(f"Error initializing pipelines: {e}")
        sys.exit(1)

def setup_logging():
    """Setup logging with rotation and formatting."""
    logger = logging.getLogger('EmailIndexer')
    logger.setLevel(logging.INFO)

    # Create a rotating file handler with compression
    handler = RotatingFileHandler(
        "indexing.log",
        maxBytes=10**7,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

def load_checkpoint(logger, CHECKPOINT_FILE="checkpoint.json"):
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

def save_checkpoint(processed_count, logger, CHECKPOINT_FILE="checkpoint.json"):
    """Save the last_processed_index to the checkpoint file."""
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"last_processed_index": processed_count}, f)
        logger.info(f"Checkpoint saved at index {processed_count}.")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

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
    return int(time.time())

def validate_message_id(message_id, subject, logger):
    """Validate that the message_id is not empty and follows a standard format.
       Assign a unique ID if invalid.
    """
    if not message_id:
        new_id = str(uuid.uuid4())
        logger.warning(f"Email without message_id found. Assigning unique ID. Email subject: {subject}")
        return new_id
    if not isinstance(message_id, str):
        new_id = str(uuid.uuid4())
        logger.warning(f"Invalid message_id type: {type(message_id)}. Assigning unique ID. Email subject: {subject}")
        return new_id
    if not message_id.startswith("<") or not message_id.endswith(">"):
        new_id = str(uuid.uuid4())
        logger.warning(f"Malformed message_id: {message_id}. Assigning unique ID. Email subject: {subject}")
        return new_id
    return message_id

def extract_labels(msg):
    """Extract labels from the email message."""
    labels = msg.get('X-Gmail-Labels', '')
    if labels:
        labels = decode_header_value(labels)
    else:
        labels = []
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

def generate_embedding(text, logger, model):
    """Generate an embedding vector for the given text using a local model."""
    try:
        embedding = model.encode(text, show_progress_bar=False)
        return embedding.tolist()  # Ensure it's serializable for Typesense
    except Exception as e:
        logger.error(f"Failed to generate embedding locally: {e}")
        return []

def perform_ner(text, logger, nlp):
    """Perform Named Entity Recognition on the given text."""
    try:
        doc = nlp(text)
        entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
        return entities
    except Exception as e:
        logger.error(f"Failed to perform NER: {e}")
        return []

def extract_keywords(text, rake):
    """Extract keywords from the given text using RAKE."""
    rake.extract_keywords_from_text(text)
    keywords = rake.get_ranked_phrases()
    return keywords

def perform_topic_modeling(texts, num_topics=10):
    """Perform Topic Modeling on a list of texts using Gensim's LDA."""
    from gensim import corpora, models
    import gensim

    # Tokenize and preprocess
    tokenized_texts = [text.lower().split() for text in texts]

    # Create a dictionary and corpus
    dictionary = corpora.Dictionary(tokenized_texts)
    corpus = [dictionary.doc2bow(text) for text in tokenized_texts]

    # Build the LDA model
    lda_model = models.LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=15)

    topics = lda_model.print_topics(num_topics=num_topics)
    return topics

def summarize_text(text, summarizer, logger, max_length=130, min_length=30):
    """Summarize text with proper truncation."""
    try:
        # Truncate input text to avoid memory issues
        text = ' '.join(text.split()[:1024])
        summary = summarizer(text, max_length=max_length, min_length=min_length, 
                           do_sample=False, batch_size=1)
        return summary[0]['summary_text']
    except Exception as e:
        logger.error(f"Failed to summarize text: {e}")
        return ""

def analyze_temporal_patterns(dates):
    """Analyze temporal patterns in email dates."""
    df = pd.DataFrame(dates, columns=['timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['hour'] = df['timestamp'].dt.hour
    hourly_counts = df['hour'].value_counts().sort_index().to_dict()
    return hourly_counts

def highlight_keywords(text, keywords):
    """Highlight keywords in the text."""
    highlighted_text = text
    for keyword in keywords:
        highlighted_text = highlighted_text.replace(keyword, f"**{keyword}**")
    return highlighted_text

def process_enrichments(body, message_id, summarizer, logger, model, nlp, rake):
    """Process enrichments in chunks with error handling."""
    enrichments = {}
    
    logger.info(f"[{message_id}] Starting enrichment processing...")
    
    try:
        logger.info(f"[{message_id}] Generating embedding vector...")
        enrichments['body_vector'] = generate_embedding(body, logger, model)
        logger.info(f"[{message_id}] Embedding vector generated successfully")
    except Exception as e:
        logger.error(f"[{message_id}] Failed to generate embedding: {e}")
        enrichments['body_vector'] = []

    try:
        logger.info(f"[{message_id}] Performing Named Entity Recognition...")
        enrichments['entities'] = perform_ner(body, logger, nlp)
        logger.info(f"[{message_id}] NER complete. Found {len(enrichments['entities'])} entities")
    except Exception as e:
        logger.error(f"[{message_id}] Failed to perform NER: {e}")
        enrichments['entities'] = []

    try:
        logger.info(f"[{message_id}] Extracting keywords...")
        enrichments['keywords'] = extract_keywords(body, rake)
        logger.info(f"[{message_id}] Keyword extraction complete. Found {len(enrichments['keywords'])} keywords")
    except Exception as e:
        logger.error(f"[{message_id}] Failed to extract keywords: {e}")
        enrichments['keywords'] = []

    # Handle longer text operations
    if len(body) > 10000:
        logger.info(f"[{message_id}] Text exceeds 10000 characters, truncating for summary...")
        body_summary = body[:10000]
    else:
        body_summary = body

    try:
        logger.info(f"[{message_id}] Generating text summary...")
        enrichments['summary'] = summarize_text(body_summary, summarizer, logger)
        logger.info(f"[{message_id}] Summary generation complete")
    except Exception as e:
        logger.error(f"[{message_id}] Failed to summarize text: {e}")
        enrichments['summary'] = ""

    logger.info(f"[{message_id}] Enrichment processing complete")
    return enrichments

def process_email(msg, summarizer, logger, model, nlp, rake):
    """Process email with memory management."""
    message_id = decode_header_value(msg.get('Message-ID', ''))
    if not message_id:
        message_id = str(uuid.uuid4())
    
    try:
        logger.info(f"[{message_id}] Starting email processing...")
        result = process_email_inner(msg, message_id, summarizer, logger, model, nlp, rake)
        
        # Force garbage collection
        gc.collect()
        
        if result:
            logger.info(f"[{message_id}] Successfully processed email")
        else:
            logger.warning(f"[{message_id}] Email processing resulted in no output")
            
        return result
        
    except Exception as e:
        logger.error(f"[{message_id}] Error processing email: {e}")
        return None

def process_email_inner(msg, message_id, summarizer, logger, model, nlp, rake):
    """Inner email processing with chunked operations."""
    try:
        # Extract basic email metadata
        logger.info(f"[{message_id}] Extracting email metadata...")
        subject = decode_header_value(msg.get('Subject', ''))
        sender = decode_header_value(msg.get('From', ''))
        recipients = decode_header_value(msg.get('To', ''))
        date_str = decode_header_value(msg.get('Date', ''))
        date_val = parse_date(date_str) if date_str else int(time.time())

        # Log extracted metadata
        logger.info(f"[{message_id}] Processing email - Subject: {subject}, From: {sender}")

        # Validate fields
        message_id = validate_message_id(message_id, subject, logger)
        if not all([subject, sender, recipients]):
            logger.warning(f"[{message_id}] Missing required fields. Skipping.")
            return None

        # Extract email body
        logger.info(f"[{message_id}] Extracting email body...")
        body = extract_email_body(msg, logger)
        if not body:
            logger.warning(f"[{message_id}] Empty body. Skipping.")
            return None
        logger.info(f"[{message_id}] Successfully extracted body (length: {len(body)} characters)")

        # Process enrichments
        logger.info(f"[{message_id}] Processing enrichments...")
        enrichments = process_enrichments(body, message_id, summarizer, logger, model, nlp, rake)
        logger.info(f"[{message_id}] Enrichments processing complete")

        # Prepare document
        logger.info(f"[{message_id}] Preparing document for indexing...")
        doc = {
            "id": message_id,
            "subject": subject,
            "sender": sender,
            "recipients": recipients,
            "date": date_val,
            "body": body,
            "labels": extract_labels(msg),
            "thread_id": extract_thread_id(msg),
            **enrichments
        }
        logger.info(f"[{message_id}] Document preparation complete")

        return doc

    except Exception as e:
        logger.error(f"[{message_id}] Failed to process email: {e}")
        return None

def import_documents_with_retries(batch, typesense_client, logger, max_retries=5):
    """
    Import documents to Typesense with a retry mechanism and detailed logging.
    Implements exponential backoff.
    """
    batch_id = str(uuid.uuid4())[:8]  # Short UUID for batch identification
    attempt = 0
    while attempt < max_retries:
        try:
            # Log batch attempt
            logger.info(f"[Batch:{batch_id}] Attempting to index batch of {len(batch)} documents (attempt {attempt + 1}/{max_retries})")
            
            # Import the batch
            response = typesense_client.collections['emails'].documents.import_(
                batch, 
                {'action': 'upsert'}
            )
            
            # Process and log the results
            success_count = 0
            error_count = 0
            error_details = []
            
            # Analyze each document result
            for i, result in enumerate(response):
                doc_id = batch[i].get('id', 'unknown')
                subject = batch[i].get('subject', 'unknown')
                
                if result.get('success', False):
                    success_count += 1
                    logger.info(f"[{doc_id}] Successfully indexed - Subject: {subject}")
                else:
                    error_count += 1
                    error = result.get('error', 'Unknown error')
                    error_details.append(f"Document ID {doc_id} ({subject}): {error}")
                    logger.error(f"[{doc_id}] Failed to index - Subject: {subject}, Error: {error}")
            
            # Log detailed summary
            logger.info(
                f"[Batch:{batch_id}] Processing complete:\n"
                f"  - Successfully indexed: {success_count}\n"
                f"  - Failed to index: {error_count}\n"
                f"  - Total processed: {len(batch)}\n"
                f"  - Success rate: {(success_count/len(batch))*100:.2f}%"
            )
            
            # If there were errors, log them in detail
            if error_details:
                logger.error(
                    f"[Batch:{batch_id}] Detailed error report:\n" + 
                    "\n".join(f"  - {error}" for error in error_details)
                )
            
            return response
            
        except Exception as e:
            attempt += 1
            wait_time = 2 ** attempt
            
            # Log the failure with more context
            logger.error(
                f"[Batch:{batch_id}] Import failed (Attempt {attempt}/{max_retries}):\n"
                f"  - Error: {str(e)}\n"
                f"  - Batch size: {len(batch)}\n"
                f"  - Waiting {wait_time} seconds before retry..."
            )
            
            # If this is the last attempt, log more details
            if attempt == max_retries:
                logger.critical(
                    f"[Batch:{batch_id}] Maximum retry attempts reached. Details:\n"
                    f"  - Last error: {str(e)}\n"
                    f"  - Batch size: {len(batch)}\n"
                    f"  - First document ID: {batch[0].get('id', 'unknown')}\n"
                    f"  - First document subject: {batch[0].get('subject', 'unknown')}"
                )
                sys.exit(1)
                
            time.sleep(wait_time)
    
    # This should never be reached due to the sys.exit(1) above
    # but including for completeness
    logger.critical(f"[Batch:{batch_id}] Failed to import after {max_retries} attempts. Exiting.")
    sys.exit(1)

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
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()
    text = soup.get_text(separator=' ')
    text = ' '.join(text.split())
    return text

def extract_email_body(msg, logger):
    """Extract email body with proper cleanup."""
    try:
        body = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_disposition() == 'attachment':
                    continue
                
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                    
                if part.get_content_type() == 'text/plain':
                    body.append(payload.decode(errors="replace"))
                elif part.get_content_type() == 'text/html':
                    body.append(strip_html(payload.decode(errors="replace")))
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                if msg.get_content_type() == 'text/plain':
                    body.append(payload.decode(errors="replace"))
                elif msg.get_content_type() == 'text/html':
                    body.append(strip_html(payload.decode(errors="replace")))
        
        # Join all body parts and clean up
        combined_body = ' '.join(body)
        
        # Remove excessive whitespace
        cleaned_body = ' '.join(combined_body.split())
        
        # Truncate if extremely long (e.g., over 100KB)
        MAX_BODY_LENGTH = 100000
        if len(cleaned_body) > MAX_BODY_LENGTH:
            logger.warning(f"Email body exceeds {MAX_BODY_LENGTH} characters. Truncating...")
            cleaned_body = cleaned_body[:MAX_BODY_LENGTH]
        
        return cleaned_body
        
    except Exception as e:
        logger.error(f"Error extracting email body: {e}")
        return ""

def index_emails(summarizer, logger, typesense_client, model, nlp, rake):
    """Main indexing function with improved memory management."""
    MBOX_PATH = "../data/mbox_export.mbox"
    CHECKPOINT_FILE = "checkpoint.json"
    BATCH_SIZE = 100
    LOG_INTERVAL = 1000

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

    last_processed_index = load_checkpoint(logger, CHECKPOINT_FILE)
    logger.info(f"Last processed index from checkpoint: {last_processed_index}")

    batch = []
    processed_count = last_processed_index

    start_index = last_processed_index + 1
    logger.info(f"Resuming from message index {start_index}.")

    # Topic modeling data
    topic_texts = []
    topic_message_ids = []

    # Reduced number of workers
    max_workers = 4

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_email = {}
            
            for i, msg in enumerate(mbox, start=1):
                if i < start_index:
                    continue

                # Submit email processing
                future = executor.submit(
                    process_email, msg, summarizer, logger, model, nlp, rake
                )
                future_to_email[future] = i

                # Process completed futures
                if len(future_to_email) >= BATCH_SIZE:
                    process_completed_futures(
                        future_to_email, batch, topic_texts, topic_message_ids,
                        processed_count, message_count, BATCH_SIZE, LOG_INTERVAL,
                        logger, typesense_client, CHECKPOINT_FILE
                    )
                    
                    # Force garbage collection after batch processing
                    gc.collect()

            # Process remaining futures
            process_completed_futures(
                future_to_email, batch, topic_texts, topic_message_ids,
                processed_count, message_count, BATCH_SIZE, LOG_INTERVAL,
                logger, typesense_client, CHECKPOINT_FILE
            )

        # Process final batch
        if batch:
            logger.info(f"Importing final batch of {len(batch)} documents...")
            import_documents_with_retries(batch, typesense_client, logger)
            save_checkpoint(processed_count, logger, CHECKPOINT_FILE)
            batch.clear()

        # Perform topic modeling if enough data
        if topic_texts:
            perform_final_analysis(topic_texts, dates, logger)

    except Exception as e:
        logger.error(f"Error during indexing: {e}")
        raise
    finally:
        # Cleanup
        gc.collect()

def process_completed_futures(future_to_email, batch, topic_texts, topic_message_ids,
                            processed_count, message_count, BATCH_SIZE, LOG_INTERVAL,
                            logger, typesense_client, CHECKPOINT_FILE):
    """Process completed futures with proper error handling."""
    for future in as_completed(future_to_email):
        try:
            result = future.result()
            if result:
                batch.append(result)
                processed_count += 1
                topic_texts.append(result['body'])
                topic_message_ids.append(result['id'])

                if processed_count % LOG_INTERVAL == 0:
                    logger.info(
                        f"Processed {processed_count}/{message_count} messages. "
                        f"Last message ID: {result['id']}, Subject: {result['subject']}"
                    )

                if len(batch) >= BATCH_SIZE:
                    logger.info(f"Importing batch of {len(batch)} documents...")
                    import_documents_with_retries(batch, typesense_client, logger)
                    save_checkpoint(processed_count, logger, CHECKPOINT_FILE)
                    batch.clear()
                    gc.collect()

        except Exception as e:
            logger.error(f"Error processing future {future_to_email[future]}: {e}")
            processed_count += 1

    future_to_email.clear()

def perform_final_analysis(topic_texts, dates, logger):
    """Perform final analysis on collected data."""
    try:
        # Topic Modeling
        logger.info("Performing Topic Modeling...")
        topics = perform_topic_modeling(topic_texts, num_topics=10)
        logger.info("Discovered Topics:")
        for topic in topics:
            logger.info(topic)

        # Temporal Analysis
        if dates:
            logger.info("Performing Temporal Analysis...")
            temporal_patterns = analyze_temporal_patterns(dates)
            logger.info(f"Temporal Patterns: {temporal_patterns}")

    except Exception as e:
        logger.error(f"Error in final analysis: {e}")

def main():
    try:
        gc.collect()
        os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'
        
        logger = setup_logging()
        logger.info("Starting email indexing process...")

        try:
            # Initialize core components
            model = initialize_local_embedding_model()
            
            try:
                nlp = spacy.load("en_core_web_sm")
            except OSError:
                logger.error("spaCy model not found. Installing...")
                os.system("python -m spacy download en_core_web_sm")
                nlp = spacy.load("en_core_web_sm")
            
            rake = Rake()
            summarizer = initialize_pipelines()

            API_KEY = os.environ.get('TYPESENSE_API_KEY', 'orion123')  # Default to orion123
            typesense_client = typesense.Client({
                'nodes': [{
                    'host': 'localhost',
                    'port': '8108',
                    'protocol': 'http'
                }],
                'api_key': API_KEY,
                'connection_timeout_seconds': 5
            })

            index_emails(
                summarizer, logger, typesense_client, model, nlp, rake
            )

        except Exception as e:
            logger.error(f"Initialization error: {e}")
            raise

    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        logger.info("Cleaning up...")
        gc.collect()
        logger.info("Process complete.")

if __name__ == "__main__":
    main()