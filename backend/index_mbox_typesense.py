"""
Optimized Email Indexing System using Weaviate v4.
Enhanced for processing 450K+ emails with improved performance, observability, and error handling.
Replaced FAISS and Typesense with Weaviate's built-in vector storage and search capabilities.
Utilizes Python's `email`, `email.utils`, and `mailbox` for parsing, SentenceTransformer for embeddings, and RAKE for keyword extraction.
"""

import os
import gc
import orjson
import time
import uuid
import logging
import psutil
import mmap
import sys
import email
import mailbox
import tempfile
import atexit

from typing import Dict, List, Optional, Any, Generator, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from pathlib import Path
import asyncio
from functools import lru_cache

# Third-party imports
import weaviate
from weaviate.classes.config import Configure, Property, DataType
import torch
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
import spacy
from rake_nltk import Rake
from dotenv import load_dotenv

# Constants
MAX_BODY_LENGTH = 100_000
BATCH_SIZE = 100
LOG_INTERVAL = 1000
MAX_RETRIES = 5
MAX_WORKERS = os.cpu_count() or 4

@dataclass
class EmailMetadata:
    """Data class for email metadata."""
    message_id: str
    subject: str
    sender: str
    recipients: str
    date_val: int
    thread_id: str
    labels: List[str]

@dataclass
class EmailEnrichments:
    """Data class for email enrichments."""
    body_vector: List[float]
    entities: List[Dict[str, str]]
    keywords: List[str]

class MLModels:
    """Container for ML models to ensure proper initialization and cleanup."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.temp_dirs = []
        atexit.register(self.cleanup_temp_directories)
        self.embedding_model: Optional[SentenceTransformer] = None
        self.nlp: Optional[Any] = None
        self.rake: Optional[Rake] = None

    def initialize(self) -> None:
        """Initialize all ML models with proper error handling."""
        try:
            self._setup_gpu_environment()
            self._initialize_embedding_model()
            self._initialize_nlp()
            self._initialize_rake()
        except Exception as e:
            self.logger.error(f"Failed to initialize ML models: {e}")
            raise

    def _setup_gpu_environment(self) -> None:
        """Configure GPU environment settings."""
        os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'
        if torch.backends.mps.is_available():
            self.logger.info("MPS (Metal Performance Shaders) is available. Using GPU for embeddings.")
        elif torch.cuda.is_available():
            torch.cuda.empty_cache()
            self.logger.info("CUDA is available. Using GPU for embeddings.")
        else:
            self.logger.warning("No GPU available. Falling back to CPU for embeddings.")

    def _initialize_embedding_model(self) -> None:
        """Initialize the sentence transformer model that produces 1536-dimensional vectors."""
        try:
            os.environ['TOKENIZERS_PARALLELISM'] = 'false'
            
            gc.collect()
            if torch.backends.mps.is_available():
                device = 'mps'
            elif torch.cuda.is_available():
                device = 'cuda'
            else:
                device = 'cpu'

            self.embedding_model = SentenceTransformer(
                'Alibaba-NLP/gte-Qwen2-1.5B-instruct',
                device=device
            )
            
            test_embedding = self.embedding_model.encode("Test text", show_progress_bar=False)
            vector_dim = len(test_embedding)
            
            if vector_dim != 1536:
                raise ValueError(f"Model produces {vector_dim}-dimensional vectors, but 1536 dimensions are required")
            
            self.logger.info(f"Embedding model initialized successfully. Vector dimensions: {vector_dim}")
        except Exception as e:
            self.logger.error(f"Failed to initialize embedding model: {e}")
            raise


    def _initialize_nlp(self) -> None:
        """Initialize spaCy model."""
        try:
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy model loaded successfully.")
        except OSError:
            self.logger.warning("SpaCy model not found, downloading...")
            os.system("python -m spacy download en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy model downloaded and loaded successfully.")

    def _initialize_rake(self) -> None:
        """Initialize RAKE keyword extractor."""
        self.rake = Rake()
        self.logger.info("RAKE keyword extractor initialized successfully.")

    def create_temp_directory(self) -> str:
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def cleanup_temp_directories(self) -> None:
        for temp_dir in self.temp_dirs:
            try:
                os.rmdir(temp_dir)
                self.logger.debug(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up temporary directory: {temp_dir}. Error: {e}")

    def cleanup(self) -> None:
        """Clean up ML models and free memory."""
        gc.collect()
        atexit.register(MLModels.cleanup_temp_directories)
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        self.logger.info("ML models cleaned up and memory freed.")

class WeaviateIndexer:
    """Handles Weaviate client connection, schema creation, and object insertion."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.client = None

    def connect(self) -> None:
        """Establish connection to Weaviate."""
        try:
            self.client = weaviate.connect_to_local()
            if not self.client.is_ready():
                raise ConnectionError("Weaviate instance is not ready.")
            self.logger.info("Connected to Weaviate successfully.")
        except Exception as e:
            self.logger.error(f"Failed to connect to Weaviate: {e}")
            raise

    def insert_object(self, email_obj: Dict[str, Any]) -> Optional[str]:
        """Insert a single email object into Weaviate."""
        try:
            collection = self.client.collections.get("Email")
            uuid = collection.data.insert(
                properties={
                    "message_id": email_obj["message_id"],
                    "subject": email_obj["subject"],
                    "sender": email_obj["sender"],
                    "recipients": email_obj["recipients"],
                    "date_val": datetime.utcfromtimestamp(email_obj["date_val"]).isoformat() + "Z",
                    "thread_id": email_obj["thread_id"],
                    "labels": email_obj["labels"],
                    "body": email_obj["body"],
                    "entities": email_obj["entities"],
                    "keywords": email_obj["keywords"],
                },
                vector=email_obj["body_vector"]  # Assuming 'body_vector' is the main vector
            )
            return uuid
        except Exception as e:
            self.logger.error(f"Failed to insert object: {e}")
            return None

    def insert_objects_batch(self, email_objs: List[Dict[str, Any]]) -> None:
        """Insert multiple email objects into Weaviate in batches."""
        try:
            collection = self.client.collections.get("Email")
            for email_obj in email_objs:
                try:
                    collection.data.insert(
                        properties={
                            "message_id": email_obj["message_id"],
                            "subject": email_obj["subject"],
                            "sender": email_obj["sender"],
                            "recipients": email_obj["recipients"],
                            "date_val": datetime.utcfromtimestamp(email_obj["date_val"]).isoformat() + "Z",
                            "thread_id": email_obj["thread_id"],
                            "labels": email_obj["labels"],
                            "body": email_obj["body"],
                            "entities": email_obj["entities"],
                            "keywords": email_obj["keywords"],
                        },
                        vector=email_obj["body_vector"]  # Assuming 'body_vector' is the main vector
                    )
                except Exception as e:
                    self.logger.error(f"Failed to insert object with message_id {email_obj['message_id']}: {e}")
        except Exception as e:
            self.logger.error(f"Failed during batch insertion: {e}")
            raise

    def close_connection(self) -> None:
        """Close the Weaviate client connection."""
        if self.client:
            self.client.close()
            self.logger.info("Weaviate client connection closed.")

class EmailProcessor:
    """Main email processing class with improved error handling and observability."""

    def __init__(self, logger: logging.Logger, ml_models: MLModels, weaviate_indexer: WeaviateIndexer):
        self.logger = logger
        self.ml_models = ml_models
        self.weaviate_indexer = weaviate_indexer
        self.checkpoint_file = Path("checkpoint.json")
        self.processed_count = self._load_checkpoint()

    def _load_checkpoint(self) -> int:
        """Load processing checkpoint with proper error handling."""
        try:
            if self.checkpoint_file.exists():
                data = orjson.loads(self.checkpoint_file.read_text())
                last_processed = data.get("last_processed_index", 0)
                self.logger.info(f"Loaded checkpoint: last_processed_index = {last_processed}")
                return last_processed
            self.logger.info("No checkpoint found. Starting from the beginning.")
            return 0
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return 0

    def _save_checkpoint(self) -> None:
        """Save processing checkpoint with error handling."""
        try:
            with open(self.checkpoint_file, 'wb') as f:
                f.write(orjson.dumps({"last_processed_index": self.processed_count}))
            self.logger.debug(f"Checkpoint saved: last_processed_index = {self.processed_count}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    def parse_email(self, raw_msg: str) -> Optional[email.message.EmailMessage]:
        """
        Parse a raw email message into a structured object using the email library.
        """
        try:
            from email import message_from_string
            msg = message_from_string(raw_msg)
            return msg
        except Exception as e:
            # Log and handle failures
            self.logger.error(f"Failed to parse email: {e}")
            return None

    def parse_email_address(self, email_str: str) -> Optional[str]:
        """Parse a single email address with or without display name using `email.utils`."""
        try:
            parsed_address = email.utils.parseaddr(email_str)
            if parsed_address[1]:  # Ensure address exists
                return parsed_address[1]
            self.logger.warning(f"Invalid email address format: {email_str}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing email address '{email_str}': {e}")
            return None

    def parse_email_addresses(self, addresses: Union[str, List[str]]) -> List[str]:
        """Parse a list of email addresses or a single address string using `email.utils`."""
        try:
            if isinstance(addresses, str):
                addresses = [addr.strip() for addr in addresses.split(",")]

            parsed_addresses = []
            for addr in addresses:
                parsed = self.parse_email_address(addr)
                if parsed:
                    parsed_addresses.append(parsed)
            return parsed_addresses
        except Exception as e:
            self.logger.error(f"Error parsing email addresses: {e}")
            return []

    def validate_email_address(self, email_str: str) -> Optional[str]:
        """Validate a single email address format using `email.utils.parseaddr`."""
        try:
            parsed = self.parse_email_address(email_str)
            if parsed:
                return parsed
            self.logger.warning(f"Invalid email address: {email_str}")
            return None
        except Exception as e:
            self.logger.error(f"Error validating email address '{email_str}': {e}")
            return None

    def validate_email_addresses(self, addresses: List[str]) -> Tuple[List[str], List[str]]:
        """Validate a list of email addresses."""
        try:
            valid_addresses, invalid_addresses = [], []
            for addr in addresses:
                if self.validate_email_address(addr):
                    valid_addresses.append(addr)
                else:
                    invalid_addresses.append(addr)
            return valid_addresses, invalid_addresses
        except Exception as e:
            self.logger.error(f"Error validating email addresses: {e}")
            return [], []

    def extract_email_metadata(self, msg: email.message.EmailMessage) -> Optional[EmailMetadata]:
        """Extract and validate email metadata using Python's `email` library."""
        try:
            # Extract headers
            sender = self.parse_email_address(msg.get("From", ""))
            recipients = self.parse_email_addresses(msg.get("To", ""))
            subject = msg.get("Subject", "")
            message_id = msg.get("Message-ID", str(uuid.uuid4()))
            date_val = int(time.time())  # Default to current time if no date provided
            
            if msg.get("Date"):
                try:
                    date_val = int(email.utils.parsedate_to_datetime(msg.get("Date")).timestamp())
                except Exception as e:
                    self.logger.warning(f"Failed to parse date header: {e}")

            thread_id = msg.get("Thread-ID", "")
            labels = msg.get("X-Gmail-Labels", "").split(",") if msg.get("X-Gmail-Labels") else []

            return EmailMetadata(
                message_id=message_id,
                subject=subject,
                sender=sender,
                recipients=recipients,
                date_val=date_val,
                thread_id=thread_id,
                labels=labels
            )
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {e}")
            return None

    def extract_email_body(self, msg: email.message.EmailMessage) -> str:
        """Extract email body using Python's `email` library."""
        try:
            if msg.is_multipart():
                body_parts = []
                for part in msg.walk():
                    if part.get_content_type() == "text/plain" and not part.get("Content-Disposition"):
                        body_parts.append(part.get_payload(decode=True).decode(errors="replace"))
                return "\n".join(body_parts)
            else:
                return msg.get_payload(decode=True).decode(errors="replace")
        except Exception as e:
            self.logger.error(f"Error extracting email body: {e}")
            return ""

    @staticmethod
    @lru_cache(maxsize=10000)
    def decode_header_value_cached(value: str) -> str:
        """Decode email header values with caching."""
        if not value:
            return ""
        try:
            from email.header import decode_header
            decoded_parts = decode_header(value)
            return " ".join(
                text.decode(charset or 'utf-8', errors='replace') if isinstance(text, bytes) else str(text)
                for text, charset in decoded_parts
            )
        except Exception as e:
            logging.error(f"Failed to decode header value: {e}")
            return value

    @staticmethod
    def strip_html(html_content: str) -> str:
        """Strip HTML tags and clean text with improved handling."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for element in soup(['script', 'style']):
                element.decompose()
            text = soup.get_text(separator=' ')
            return ' '.join(text.split())
        except Exception as e:
            logging.error(f"Error stripping HTML: {e}")
            return html_content

    def process_email(self, msg: Any) -> Optional[Dict[str, Any]]:
        """Process single email with comprehensive error handling."""
        try:
            metadata = self._extract_metadata(msg)
            if not metadata:
                return None

            body = self.extract_email_body(msg)
            if not body:
                return None

            enrichments = self._process_enrichments(body, metadata.message_id)
            if not enrichments:
                return None

            email_dict = {
                "id": metadata.message_id,  # Using message_id as UUID
                "message_id": metadata.message_id,
                "subject": metadata.subject,
                "sender": metadata.sender,
                "recipients": metadata.recipients,
                "date_val": metadata.date_val,
                "thread_id": metadata.thread_id,
                "labels": metadata.labels,
                "body": body,
                "entities": enrichments.entities,
                "keywords": enrichments.keywords,
                "body_vector": enrichments.body_vector
            }

            return email_dict

        except Exception as e:
            self.logger.error(f"Error processing email: {e}")
            return None

    def _extract_metadata(self, msg: Any) -> Optional[EmailMetadata]:
        """Extract and validate email metadata."""
        try:
            message_id = self.decode_header_value_cached(msg.headers.get('Message-ID', ''))
            subject = self.decode_header_value_cached(msg.headers.get('Subject', ''))
            sender = self.decode_header_value_cached(msg.headers.get('From', ''))
            recipients = self.decode_header_value_cached(msg.headers.get('To', ''))
            
            if not all([subject, sender, recipients]):
                return None
                
            date_str = self.decode_header_value_cached(msg.headers.get('Date', ''))
            date_val = self.parse_date(date_str) if date_str else int(time.time())
            
            return EmailMetadata(
                message_id=message_id or str(uuid.uuid4()),
                subject=subject,
                sender=sender,
                recipients=recipients,
                date_val=date_val,
                thread_id=self.decode_header_value_cached(msg.headers.get('Thread-Id', '')),
                labels=self._extract_labels(msg)
            )
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {e}")
            return None

    def _extract_labels(self, msg: Any) -> List[str]:
        """Extract email labels with proper handling."""
        labels = self.decode_header_value_cached(msg.headers.get('X-Gmail-Labels', ''))
        return [label.strip() for label in labels.split(',')] if labels else []

    def _process_enrichments(self, body: str, message_id: str) -> Optional[EmailEnrichments]:
        """Process email enrichments with comprehensive error handling."""
        try:
            self.logger.debug(f"[{message_id}] Processing enrichments...")
            
            body_vector = self._generate_embedding(body, message_id)
            entities = self._perform_ner(body, message_id)
            keywords = self._extract_keywords(body, message_id)
            
            return EmailEnrichments(
                body_vector=body_vector,
                entities=entities,
                keywords=keywords
            )
        except Exception as e:
            self.logger.error(f"[{message_id}] Failed to process enrichments: {e}")
            return None

    def _generate_embedding(self, text: str, message_id: str) -> List[float]:
        """Generate embedding vector with error handling and dimension verification."""
        try:
            # Compute hash of the email body
            body_hash = hash(text)
            # Check if embedding is cached
            cached_embedding = self.embedding_cache.get(body_hash)
            if cached_embedding:
                self.logger.debug(f"[{message_id}] Using cached embedding.")
                return cached_embedding
            
            vector = self.ml_models.embedding_model.encode(
                text, show_progress_bar=False
            ).tolist()
            
            # Verify vector dimensions
            if len(vector) != 1536:
                raise ValueError(f"Generated vector has {len(vector)} dimensions, expected 1536")
                
            # Cache the embedding
            self.embedding_cache[body_hash] = vector
            
            return vector
        except Exception as e:
            self.logger.error(f"[{message_id}] Failed to generate embedding: {e}")
            # Return a zero vector of correct dimension instead of empty list
            return [0.0] * 1536

    def _perform_ner(self, text: str, message_id: str) -> List[Dict[str, str]]:
        """Perform Named Entity Recognition with error handling."""
        try:
            doc = self.ml_models.nlp(text)
            return [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
        except Exception as e:
            self.logger.error(f"[{message_id}] Failed to perform NER: {e}")
            return []

    def _extract_keywords(self, text: str, message_id: str) -> List[str]:
        """Extract keywords with error handling."""
        try:
            self.ml_models.rake.extract_keywords_from_text(text)
            return self.ml_models.rake.get_ranked_phrases()
        except Exception as e:
            self.logger.error(f"[{message_id}] Failed to extract keywords: {e}")
            return []

    # Implementing Embedding Cache (2.9.1)
    def initialize_embedding_cache(self):
        """Initialize a simple in-memory cache for embeddings."""
        self.embedding_cache: Dict[int, List[float]] = {}

    # Additional methods for embedding cache persistence can be added here

class EmailIndexer:
    """Main class for email indexing process."""
    
    def __init__(self, mbox_path: str):
        self.mbox_path = Path(mbox_path)
        self.logger, self.listener = self.setup_logging()
        self.ml_models = MLModels(self.logger)
        self.weaviate_indexer = WeaviateIndexer(self.logger)
        self.email_processor = EmailProcessor(
            self.logger,
            self.ml_models,
            self.weaviate_indexer
        )
        self.email_processor.initialize_embedding_cache()

    def setup_logging(self) -> Tuple[logging.Logger, QueueListener]:
        """Setup asynchronous logging with rotation and formatting."""
        logger = logging.getLogger('EmailIndexer')
        logger.setLevel(logging.INFO)
        
        # Use standard Queue 
        from queue import Queue
        log_queue = Queue()
        
        # Create and add handlers
        handler = RotatingFileHandler(
            "indexing.log",
            maxBytes=10**7,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Create and start queue handler
        queue_handler = QueueHandler(log_queue)
        logger.addHandler(queue_handler)
        
        # Create and start queue listener
        listener = QueueListener(log_queue, handler)
        listener.start()
        
        return logger, listener

    def cleanup(self):
        """Cleanup resources properly."""
        try:
            # Stop the queue listener first
            if hasattr(self, 'listener') and self.listener:
                try:
                    self.listener.stop()
                    delattr(self, 'listener')  # Remove the listener after stopping
                except Exception as e:
                    self.logger.error(f"Error stopping listener: {e}")

            # Close all log handlers
            if hasattr(self, 'logger') and self.logger:
                for handler in self.logger.handlers[:]:
                    try:
                        handler.close()
                        self.logger.removeHandler(handler)
                    except Exception as e:
                        self.logger.error(f"Error closing handler: {e}")
            
            # Close Weaviate connection
            if hasattr(self, 'weaviate_indexer') and self.weaviate_indexer:
                try:
                    self.weaviate_indexer.close_connection()
                except Exception as e:
                    self.logger.error(f"Error closing Weaviate connection: {e}")

            # Cleanup ML models
            if hasattr(self, 'ml_models') and self.ml_models:
                try:
                    self.ml_models.cleanup()
                except Exception as e:
                    self.logger.error(f"Error cleaning up ML models: {e}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def _validate_environment(self) -> None:
        """Validate environment and requirements."""
        if not self.mbox_path.exists():
            raise FileNotFoundError(f"MBOX file not found at {self.mbox_path}")
        
        # Validate Weaviate connection
        try:
            self.weaviate_indexer.connect()
            self.logger.info("Successfully connected to Weaviate.")
        except Exception as e:
            self.logger.error(f"Failed to connect or create schema in Weaviate: {e}")
            raise

    def _get_memory_usage(self) -> str:
        """Get current memory usage information."""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return f"Memory RSS: {memory_info.rss / 1024 / 1024:.2f}MB, VMS: {memory_info.vms / 1024 / 1024:.2f}MB"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def __del__(self):
        self.cleanup()

    def process_emails(self) -> None:
        """Process emails with improved batch handling and observability."""
        try:
            self.logger.info(f"Opening mbox file at {self.mbox_path}")
            self.logger.info(f"Initial memory usage - {self._get_memory_usage()}")

            email_messages = []
            mbox = None
            try:
                mbox = mailbox.mbox(self.mbox_path)
                for key in mbox.keys():
                    try:
                        msg = mbox[key]
                        raw_msg = msg.as_string()
                        email_messages.append(self.email_processor.parse_email(raw_msg))
                    except Exception as e:
                        self.logger.error(f"Failed to parse email with key {key}: {e}")
            except Exception as e:
                self.logger.error(f"Failed to read mbox file: {e}")
                raise
            finally:
                if mbox:
                    mbox.close()

            self.logger.info(f"Successfully read {len(email_messages)} messages from mbox file.")

            batch: List[Dict[str, Any]] = []
            start_time = time.time()

            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self.email_processor.process_email, msg): msg
                    for msg in email_messages[self.email_processor.processed_count:]
                }

                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        batch.append(result)
                        self.email_processor.processed_count += 1

                        if self.email_processor.processed_count % LOG_INTERVAL == 0:
                            self._log_progress()

                        if len(batch) >= BATCH_SIZE:
                            self.weaviate_indexer.insert_objects_batch(batch)
                            self.email_processor._save_checkpoint()
                            self.logger.info(f"Inserted batch of {len(batch)} emails into Weaviate.")
                            batch.clear()
                            gc.collect()

                if batch:
                    self.weaviate_indexer.insert_objects_batch(batch)
                    self.email_processor._save_checkpoint()
                    self.logger.info(f"Inserted final batch of {len(batch)} emails into Weaviate.")
                    batch.clear()

            total_time = time.time() - start_time
            self.logger.info(f"Email processing completed in {total_time:.2f} seconds.")

        except Exception as e:
            self.logger.error(f"Error processing emails: {e}")
            raise

    def _log_progress(self) -> None:
        """Log processing progress with detailed metrics."""
        progress = (self.email_processor.processed_count / 450_000) * 100  # Assuming total is 450K
        self.logger.info(
            f"Progress: {self.email_processor.processed_count}/450000 "
            f"messages ({progress:.1f}%)"
        )

    def run(self) -> None:
        """Main execution method with proper initialization and cleanup."""
        try:
            self.logger.info("Starting email indexing process...")
            
            # Initialize environment
            self._validate_environment()
            
            # Initialize ML models
            self.ml_models.initialize()
            
            # Process emails
            self.process_emails()
            
            self.logger.info("Email indexing process completed successfully.")
            
        except Exception as e:
            self.logger.error(f"Fatal error in indexing process: {e}")
            raise

    @staticmethod
    def parse_date(date_str: str) -> int:
        """Parse date string to Unix timestamp with error handling."""
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%d %b %Y %H:%M:%S %z"
        ]
        
        for fmt in formats:
            try:
                return int(datetime.strptime(date_str.strip(), fmt).timestamp())
            except ValueError:
                continue
        return int(time.time())

def main():
    """Main entry point with proper error handling."""
    try:
        load_dotenv()
        with EmailIndexer("../data/mbox_export.mbox") as indexer:
            indexer.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
