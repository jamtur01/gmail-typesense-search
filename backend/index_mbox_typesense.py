"""
Optimized Email Indexing System using Weaviate v4.
Enhanced for processing 450K+ emails with improved performance, observability, and error handling.
Replaced FAISS and Typesense with Weaviate's built-in vector storage and search capabilities.
Utilizes Python's `email`, `email.utils`, and `mailbox` for parsing, SentenceTransformer for embeddings, 
spaCy for NER, KeyBERT for keyword extraction, and cProfile for profiling.
"""

import os
import gc
import orjson
import time
import uuid
import logging
import psutil
import sys
import email
import mailbox
import tempfile
import atexit
import cProfile
import pstats

from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
# Using ThreadPoolExecutor by default (you can switch to process-based if desired, 
# but that requires careful pickling if you have queue-based logging).
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
from pathlib import Path
import asyncio
from functools import lru_cache

# Third-party imports
import weaviate
from weaviate.classes.config import Configure, Property, DataType
import torch
import spacy
from sentence_transformers import SentenceTransformer
from keybert import KeyBERT
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
    """
    Container for ML models to ensure proper initialization and cleanup:
    - SentenceTransformer for embeddings
    - spaCy for NER
    - KeyBERT for keyword extraction
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.temp_dirs = []
        atexit.register(self.cleanup_temp_directories)
        self.embedding_model: Optional[SentenceTransformer] = None
        self.nlp: Optional[Any] = None
        self.keybert: Optional[KeyBERT] = None

    def initialize(self) -> None:
        """Initialize all ML models with proper error handling."""
        try:
            self._initialize_embedding_model()
            self._initialize_nlp()
            self._initialize_keybert()
        except Exception as e:
            self.logger.error(f"Failed to initialize ML models: {e}")
            raise

    def _initialize_embedding_model(self) -> None:
        """Initialize the sentence transformer model that produces 1536-dimensional vectors."""
        try:
            os.environ['TOKENIZERS_PARALLELISM'] = 'false'
            gc.collect()

            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            self.logger.info(f"Loading SentenceTransformer on device={device}")

            self.embedding_model = SentenceTransformer(
                'Alibaba-NLP/gte-Qwen2-1.5B-instruct',
                device=device
            )
            test_embedding = self.embedding_model.encode(["Test text"], show_progress_bar=False)
            if len(test_embedding[0]) != 1536:
                raise ValueError("Model must produce 1536-dimensional vectors.")
            self.logger.info("Embedding model initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize embedding model: {e}")
            raise

    def _initialize_nlp(self) -> None:
        """Initialize spaCy model."""
        try:
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy model loaded successfully.")
        except OSError:
            self.logger.warning("spaCy model not found, downloading...")
            os.system("python -m spacy download en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy model downloaded and loaded successfully.")

    def _initialize_keybert(self) -> None:
        """Initialize KeyBERT for keyword extraction."""
        try:
            self.keybert = KeyBERT(model=self.embedding_model)
            self.logger.info("KeyBERT keyword extractor initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize KeyBERT: {e}")
            raise

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
        atexit.register(self.cleanup_temp_directories)
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
            new_uuid = collection.data.insert(
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
                vector=email_obj["body_vector"] 
            )
            return new_uuid
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
                        vector=email_obj["body_vector"]  # main vector
                    )
                except Exception as ee:
                    self.logger.error(f"Failed to insert object with message_id {email_obj['message_id']}: {ee}")
        except Exception as e:
            self.logger.error(f"Failed during batch insertion: {e}")
            raise

    def close_connection(self) -> None:
        """Close the Weaviate client connection."""
        if self.client:
            self.client.close()
            self.logger.info("Weaviate client connection closed.")


class EmailProcessor:
    """
    Main email processing class that:
     - Batches parsing
     - Batches spaCy for NER
     - Batches embeddings
     - Uses KeyBERT for keywords
     - Avoids parsing the same email more than once
    """

    def __init__(self, logger: logging.Logger, ml_models: MLModels, weaviate_indexer: WeaviateIndexer):
        self.logger = logger
        self.ml_models = ml_models
        self.weaviate_indexer = weaviate_indexer

        # Checkpointing
        self.checkpoint_file = Path("checkpoint.json")
        self.processed_count = self._load_checkpoint()

        # Cache for raw parsing results (avoid re-parsing the same raw)
        self.parsed_cache: Dict[int, Dict[str, Any]] = {}

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

    def parse_email_single(self, raw_msg: str) -> Optional[email.message.EmailMessage]:
        """Parse a single raw email message."""
        try:
            from email import message_from_string
            return message_from_string(raw_msg)
        except Exception as e:
            self.logger.error(f"Failed to parse email: {e}")
            return None

    def extract_email_metadata(self, msg: email.message.EmailMessage) -> Optional[EmailMetadata]:
        """Extract minimal metadata using Python's `email` library."""
        try:
            # basic headers
            sender = msg.get("From", "")
            recipients = msg.get("To", "")
            subject = msg.get("Subject", "")
            message_id = msg.get("Message-ID", str(uuid.uuid4()))
            date_val = int(time.time())  # fallback
            if msg.get("Date"):
                try:
                    date_val = int(email.utils.parsedate_to_datetime(msg.get("Date")).timestamp())
                except Exception:
                    pass
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
        """Extract text/plain body."""
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

    def batch_parse_raw_messages(self, raw_messages: List[str]) -> Tuple[List[Optional[EmailMetadata]], List[str]]:
        """
        For each raw email, parse and extract (metadata, body) exactly once.
        Return parallel lists: [metadata_1, ...], [body_1, ...].
        """
        meta_list = []
        body_list = []

        for raw in raw_messages:
            rhash = hash(raw)
            if rhash in self.parsed_cache:
                # Reuse
                cached = self.parsed_cache[rhash]
                meta_list.append(cached["metadata"])
                body_list.append(cached["body"])
                continue

            msg_obj = self.parse_email_single(raw)
            if not msg_obj:
                meta_list.append(None)
                body_list.append("")
                continue

            meta = self.extract_email_metadata(msg_obj)
            body = self.extract_email_body(msg_obj)

            self.parsed_cache[rhash] = {
                "metadata": meta,
                "body": body
            }
            meta_list.append(meta)
            body_list.append(body)

        return meta_list, body_list

    def batch_enrich(self, meta_list: List[Optional[EmailMetadata]], body_list: List[str]) -> List[Dict[str, Any]]:
        """
        Given parallel lists of metadata and body, run:
         - batched embeddings,
         - batched spaCy NER,
         - KeyBERT for keywords (per doc).
        Return a list of enriched email dicts, ready for Weaviate insertion.
        """
        # 1. Batched embeddings
        # Convert None or empty to ""
        texts_for_embedding = [txt if txt else "" for txt in body_list]
        embeddings = self.ml_models.embedding_model.encode(
            texts_for_embedding,
            batch_size=32,
            show_progress_bar=False
        )

        # 2. Batched spaCy NER using nlp.pipe
        docs = list(
            self.ml_models.nlp.pipe(texts_for_embedding, batch_size=32)
        )

        # 3. KeyBERT in a loop (KeyBERT doesn't have a big batch method, 
        #    but it reuses the same model internally)
        results = []
        for idx, (meta, text) in enumerate(zip(meta_list, body_list)):
            if not meta:
                continue

            # Entities from spaCy doc
            ents = [{"text": ent.text, "label": ent.label_} for ent in docs[idx].ents]

            # Keywords from KeyBERT, e.g. top 5
            if text:
                kwpairs = self.ml_models.keybert.extract_keywords(text, top_n=5)
                keywords = [p[0] for p in kwpairs]
            else:
                keywords = []

            email_dict = {
                "id": meta.message_id,
                "message_id": meta.message_id,
                "subject": meta.subject,
                "sender": meta.sender,
                "recipients": meta.recipients,
                "date_val": meta.date_val,
                "thread_id": meta.thread_id,
                "labels": meta.labels,
                "body": text,
                "entities": ents,
                "keywords": keywords,
                "body_vector": embeddings[idx].tolist()
            }
            results.append(email_dict)
        return results

class EmailIndexer:
    """Main class for email indexing process."""
    
    def __init__(self, mbox_path: str):
        self.mbox_path = Path(mbox_path)
        self.logger = self.setup_logging()
        
        self.ml_models = MLModels(self.logger)
        self.weaviate_indexer = WeaviateIndexer(self.logger)
        self.email_processor = EmailProcessor(
            self.logger,
            self.ml_models,
            self.weaviate_indexer
        )

    def setup_logging(self) -> logging.Logger:
        """
        Simple RotatingFileHandler-based logging.
        """
        logger = logging.getLogger('EmailIndexer')
        logger.setLevel(logging.INFO)
        
        # If there is already a handler, just return it
        if logger.handlers:
            return logger

        handler = RotatingFileHandler(
            "indexing.log",
            maxBytes=10**7,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def cleanup(self):
        """Cleanup resources properly."""
        try:
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
        """
        Process emails with:
         - cProfile profiling
         - batching spaCy, embeddings, KeyBERT
        """
        profiler = cProfile.Profile()
        profiler.enable()

        try:
            self.logger.info(f"Opening mbox file at {self.mbox_path}")
            self.logger.info(f"Initial memory usage - {self._get_memory_usage()}")

            # Read the entire mailbox into a list of raw messages
            email_messages = []
            mbox = None
            try:
                mbox = mailbox.mbox(self.mbox_path)
                total_emails = len(mbox)
                self.logger.info(f"Total emails in mbox: {total_emails}")

                for key, msg in mbox.iteritems():
                    try:
                        raw_msg = msg.as_string()
                        email_messages.append(raw_msg)
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

            # We'll use a ThreadPoolExecutor with batch chunks
            # to process large sets efficiently
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for i in range(self.email_processor.processed_count, len(email_messages), BATCH_SIZE):
                    chunk = email_messages[i : i + BATCH_SIZE]

                    # We'll do the parse + enrichment in a single function call
                    future = executor.submit(self._process_chunk, chunk)
                    # Wait for the chunk to complete
                    result_list = future.result()

                    # Insert the batch into Weaviate
                    self.weaviate_indexer.insert_objects_batch(result_list)
                    # Update checkpoint
                    self.email_processor.processed_count += len(result_list)
                    self.email_processor._save_checkpoint()

                    self.logger.info(f"Inserted chunk of {len(result_list)} emails into Weaviate.")
                    gc.collect()

                    # Log progress every LOG_INTERVAL messages
                    if self.email_processor.processed_count % LOG_INTERVAL == 0:
                        self._log_progress()
                        self.logger.info(f"Current memory usage: {self._get_memory_usage()}")

            total_time = time.time() - start_time
            self.logger.info(f"Email processing completed in {total_time:.2f} seconds.")

        except Exception as e:
            self.logger.error(f"Error processing emails: {e}")
            raise

        # Stop cProfile
        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats("cumulative")
        # Dump to file for offline analysis with snakeviz or pstats
        stats.dump_stats("email_indexing_profile.prof")
        self.logger.info("Profile data saved to email_indexing_profile.prof")

    def _process_chunk(self, raw_msgs: List[str]) -> List[Dict[str, Any]]:
        """
        Single function that:
          - Batches parse the raw messages
          - Batches embed, do NER, and keywords
          - Returns result dicts
        """
        meta_list, body_list = self.email_processor.batch_parse_raw_messages(raw_msgs)
        enriched = self.email_processor.batch_enrich(meta_list, body_list)
        return enriched

    def _log_progress(self) -> None:
        """Log processing progress with detailed metrics (assuming 450k total)."""
        progress = (self.email_processor.processed_count / 450_000) * 100  
        self.logger.info(
            f"Progress: {self.email_processor.processed_count}/450000 "
            f"messages ({progress:.1f}%)"
        )

    def run(self) -> None:
        """Main execution method with proper initialization and cleanup."""
        try:
            self.logger.info("Starting email indexing process...")
            
            # Validate environment
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
    """Main entry point with error handling."""
    try:
        load_dotenv()
        with EmailIndexer("../data/mbox_export.mbox") as indexer:
            indexer.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
