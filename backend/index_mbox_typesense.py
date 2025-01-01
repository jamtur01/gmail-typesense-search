"""
Optimized Email Indexing System for large mbox files (17GB+).
Implements:
1) Streaming the mbox in chunks,
2) Avoiding creation of large intermediate lists,
3) Parallel reading & processing using a Queue with a reader thread and multiple worker threads.

Other features:
- spaCy for NER,
- SentenceTransformer for embeddings,
- KeyBERT for keywords,
- cProfile for profiling,
- Weaviate for vector storage and search.
"""

import os
import gc
import sys
import time
import uuid
import queue
import email
import mailbox
import tempfile
import atexit
import cProfile
import pstats
import threading
import logging
import psutil

from typing import (
    Dict, List, Optional, Any, Tuple, Union
)
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from functools import lru_cache

# Third-party
import orjson
import torch
import spacy
from sentence_transformers import SentenceTransformer
from keybert import KeyBERT
from dotenv import load_dotenv

# Weaviate imports (unchanged)
import weaviate
from weaviate.classes.config import Configure, Property, DataType

from logging.handlers import RotatingFileHandler


# -------------------- Constants & Configuration --------------------
MAX_BODY_LENGTH = 100_000
BATCH_SIZE = 250        # how many raw emails per chunk read from mailbox
LOG_INTERVAL = 100
MAX_WORKERS = 2
QUEUE_MAX_SIZE = 5      # how many chunks can be queued at once

STOP_MARKER = None       # sentinel for worker threads to stop

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


# -------------------- MLModels Class --------------------
class MLModels:
    """
    Container for ML models:
     - SentenceTransformer (embeddings)
     - spaCy (NER)
     - KeyBERT (keywords)
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.temp_dirs = []
        atexit.register(self.cleanup_temp_directories)
        self.embedding_model: Optional[SentenceTransformer] = None
        self.nlp: Optional[Any] = None
        self.keybert: Optional[KeyBERT] = None

    def initialize(self) -> None:
        """Initialize all ML models."""
        try:
            self._initialize_embedding_model()
            self._initialize_nlp()
            self._initialize_keybert()
        except Exception as e:
            self.logger.error(f"Failed to initialize ML models: {e}")
            raise

    def _initialize_embedding_model(self) -> None:
        os.environ['TOKENIZERS_PARALLELISM'] = 'false'
        gc.collect()

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.logger.info(f"Loading SentenceTransformer on device={device}")

        model_name = "Alibaba-NLP/gte-Qwen2-1.5B-instruct"
        self.embedding_model = SentenceTransformer(model_name, device=device)
        
        # Quick dimension check
        test_embedding = self.embedding_model.encode(["Test text"], show_progress_bar=False)
        if len(test_embedding[0]) != 1536:
            raise ValueError("Model should produce 1536-dimensional vectors")
        self.logger.info("Embedding model initialized successfully.")

    def _initialize_nlp(self) -> None:
        try:
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy model loaded successfully.")
        except OSError:
            self.logger.warning("spaCy model not found; downloading...")
            os.system("python -m spacy download en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy model downloaded and loaded successfully.")

    def _initialize_keybert(self) -> None:
        self.logger.info("Initializing KeyBERT...")
        self.keybert = KeyBERT(model=self.embedding_model)
        self.logger.info("KeyBERT initialized successfully.")

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
                self.logger.warning(f"Failed to clean up temp dir: {temp_dir} => {e}")

    def cleanup(self) -> None:
        """Clean up ML models and free memory."""
        gc.collect()
        atexit.register(self.cleanup_temp_directories)
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        self.logger.info("ML models cleaned up and memory freed.")


# -------------------- WeaviateIndexer (unchanged) --------------------
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
                        vector=email_obj["body_vector"]
                    )
                except Exception as ee:
                    self.logger.error(f"Failed to insert {email_obj['message_id']}: {ee}")
        except Exception as e:
            self.logger.error(f"Failed during batch insertion: {e}")
            raise

    def close_connection(self) -> None:
        if self.client:
            self.client.close()
            self.logger.info("Weaviate client connection closed.")


# -------------------- EmailProcessor (batched, KeyBERT, etc.) --------------------
class EmailProcessor:
    """
    Main email processing:
      - parse raw messages (metadata + body)
      - batch embeddings & spaCy NER
      - KeyBERT for keywords
      - caching to avoid re-parse
    """
    def __init__(self, logger: logging.Logger, ml_models: MLModels, weaviate_indexer: WeaviateIndexer):
        self.logger = logger
        self.ml_models = ml_models
        self.weaviate_indexer = weaviate_indexer

        # checkpoint
        self.checkpoint_file = Path("checkpoint.json")
        self.processed_count = self._load_checkpoint()

        # cache for parsed raw
        self.parsed_cache: Dict[int, Dict[str, Any]] = {}

    def _load_checkpoint(self) -> int:
        """Load processing checkpoint with error handling."""
        try:
            if self.checkpoint_file.exists():
                data = orjson.loads(self.checkpoint_file.read_text())
                last_processed = data.get("last_processed_index", 0)
                self.logger.info(f"Loaded checkpoint: last_processed_index = {last_processed}")
                return last_processed
            self.logger.info("No checkpoint found. Starting at 0.")
            return 0
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return 0

    def _save_checkpoint(self) -> None:
        """Save processing checkpoint with error handling."""
        try:
            with open(self.checkpoint_file, "wb") as f:
                f.write(orjson.dumps({"last_processed_index": self.processed_count}))
            self.logger.debug(f"Checkpoint saved: {self.processed_count}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    def parse_email_single(self, raw_msg: str) -> Optional[email.message.EmailMessage]:
        try:
            from email import message_from_string
            return message_from_string(raw_msg)
        except Exception as e:
            self.logger.error(f"Failed to parse email: {e}")
            return None

    def extract_email_metadata(self, msg: email.message.EmailMessage) -> Optional[EmailMetadata]:
        try:
            from email.utils import parsedate_to_datetime
            sender = msg.get("From", "")
            recipients = msg.get("To", "")
            subject = msg.get("Subject", "")
            message_id = msg.get("Message-ID", str(uuid.uuid4()))
            date_val = int(time.time())  # fallback
            if msg.get("Date"):
                try:
                    date_val = int(parsedate_to_datetime(msg["Date"]).timestamp())
                except:
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
            self.logger.error(f"Error extracting body: {e}")
            return ""

    def batch_parse_raw_messages(self, raw_msgs: List[str]) -> Tuple[List[Optional[EmailMetadata]], List[str]]:
        """
        For each raw email, parse once, store in cache => returns parallel lists
        of EmailMetadata and body strings.
        """
        meta_list: List[Optional[EmailMetadata]] = []
        body_list: List[str] = []

        for raw in raw_msgs:
            raw_hash = hash(raw)
            if raw_hash in self.parsed_cache:
                cached = self.parsed_cache[raw_hash]
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

            self.parsed_cache[raw_hash] = {
                "metadata": meta,
                "body": body
            }
            meta_list.append(meta)
            body_list.append(body)
        return meta_list, body_list

    def batch_enrich(self, meta_list: List[Optional[EmailMetadata]], body_list: List[str]) -> List[Dict[str, Any]]:
        """
        Runs embeddings, spaCy NER, KeyBERT on all bodies in a single pass. 
        Returns final dicts for Weaviate insertion.
        """
        # 1. Embeddings
        inputs = [b if b else "" for b in body_list]
        embeddings = self.ml_models.embedding_model.encode(inputs, batch_size=32, show_progress_bar=False)

        # 2. spaCy NER in a single batch
        docs = list(self.ml_models.nlp.pipe(inputs, batch_size=32))

        # 3. KeyBERT in a loop
        results: List[Dict[str, Any]] = []
        for idx, (meta, txt) in enumerate(zip(meta_list, body_list)):
            if not meta:
                continue

            ents = [{"text": ent.text, "label": ent.label_} for ent in docs[idx].ents]

            keywords = []
            if txt:
                pairs = self.ml_models.keybert.extract_keywords(txt, top_n=5)
                keywords = [p[0] for p in pairs]

            email_dict = {
                "id": meta.message_id,
                "message_id": meta.message_id,
                "subject": meta.subject,
                "sender": meta.sender,
                "recipients": meta.recipients,
                "date_val": meta.date_val,
                "thread_id": meta.thread_id,
                "labels": meta.labels,
                "body": txt,
                "entities": ents,
                "keywords": keywords,
                "body_vector": embeddings[idx].tolist(),
            }
            results.append(email_dict)

        return results

    def process_chunk(self, raw_msgs: List[str]) -> None:
        """
        Parse and enrich a chunk of raw emails, then insert into Weaviate.
        Increments processed_count and updates checkpoint.
        """
        # parse
        meta_list, body_list = self.batch_parse_raw_messages(raw_msgs)
        # enrich
        enriched = self.batch_enrich(meta_list, body_list)
        # insert
        self.weaviate_indexer.insert_objects_batch(enriched)

        # update checkpoint
        self.processed_count += len(enriched)
        self._save_checkpoint()
        self.logger.info(f"Processed+Inserted {len(enriched)} emails; total so far: {self.processed_count}")


# -------------------- EmailIndexer with parallel read & process --------------------
class EmailIndexer:
    """
    Main class controlling:
     - environment validation,
     - reading large mbox in chunks,
     - queue-based concurrency,
     - cProfile for performance,
     - final run.
    """
    
    def __init__(self, mbox_path: str):
        self.mbox_path = Path(mbox_path)
        self.logger = self.setup_logging()

        self.ml_models = MLModels(self.logger)
        self.weaviate_indexer = WeaviateIndexer(self.logger)
        self.email_processor = EmailProcessor(self.logger, self.ml_models, self.weaviate_indexer)

        # A thread-safe queue for chunks of raw emails
        self.chunk_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)

        # We'll hold references to worker threads
        self.workers: List[threading.Thread] = []

        # Stop marker
        self.stop_flag = False

    def setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("EmailIndexer")
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            handler = RotatingFileHandler(
                "indexing.log", maxBytes=10**7, backupCount=5, encoding="utf-8"
            )
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s] %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def cleanup(self):
        """Cleanup resources properly."""
        try:
            # stop worker threads if needed
            self.stop_flag = True

            # attempt to close weaviate, logger, etc.
            if hasattr(self, "logger") and self.logger:
                for handler in self.logger.handlers[:]:
                    try:
                        handler.close()
                        self.logger.removeHandler(handler)
                    except Exception as e:
                        print(f"Error closing handler: {e}", file=sys.stderr)

            if hasattr(self, 'weaviate_indexer') and self.weaviate_indexer:
                try:
                    self.weaviate_indexer.close_connection()
                except Exception as e:
                    print(f"Error closing Weaviate connection: {e}", file=sys.stderr)

            if hasattr(self, 'ml_models') and self.ml_models:
                try:
                    self.ml_models.cleanup()
                except Exception as e:
                    print(f"Error cleaning up ML models: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Error during cleanup: {e}", file=sys.stderr)

    def _validate_environment(self) -> None:
        if not self.mbox_path.exists():
            raise FileNotFoundError(f"MBOX file not found at {self.mbox_path}")
        self.weaviate_indexer.connect()
        self.logger.info("Environment validated; Weaviate connection successful.")

    def _reader_thread(self) -> None:
        """
        Reads the big mbox in chunks of BATCH_SIZE raw emails,
        puts them into chunk_queue until done, then signals STOP_MARKER.
        """
        try:
            self.logger.info(f"Reader thread started, opening {self.mbox_path}")
            mbox = mailbox.mbox(self.mbox_path)
            batch_raw: List[str] = []

            for idx, msg in enumerate(mbox):
                if self.stop_flag:
                    break

                try:
                    raw_msg = msg.as_string()
                except Exception as e:
                    self.logger.error(f"Failed to parse email at index {idx}: {e}")
                    continue

                batch_raw.append(raw_msg)

                if len(batch_raw) >= BATCH_SIZE:
                    # Put chunk into the queue
                    self.chunk_queue.put(batch_raw, block=True)
                    self.logger.debug(f"Reader queued chunk of size {len(batch_raw)}")
                    batch_raw = []

            # If leftover
            if batch_raw and not self.stop_flag:
                self.chunk_queue.put(batch_raw, block=True)
                self.logger.debug(f"Reader queued final chunk of size {len(batch_raw)}")

        except Exception as e:
            self.logger.error(f"Reader thread exception: {e}")
        finally:
            # signal workers to stop
            self.chunk_queue.put(STOP_MARKER)
            self.logger.info("Reader thread finished; STOP_MARKER placed.")

    def _worker_thread(self, worker_id: int) -> None:
        """
        Worker thread:
         - pulls chunk from chunk_queue
         - calls email_processor.process_chunk
         - repeats until STOP_MARKER
        """
        self.logger.info(f"Worker-{worker_id} started.")
        while not self.stop_flag:
            try:
                chunk = self.chunk_queue.get(timeout=2)
            except queue.Empty:
                # check if stop_flag, else continue
                if self.stop_flag:
                    break
                continue

            if chunk is STOP_MARKER:
                self.logger.info(f"Worker-{worker_id} received STOP_MARKER.")
                self.chunk_queue.put(STOP_MARKER)  # pass sentinel to other workers
                break

            # Process chunk
            self.email_processor.process_chunk(chunk)
            self.chunk_queue.task_done()

            if self.email_processor.processed_count % LOG_INTERVAL == 0:
                self._log_progress()
                self.logger.info(f"Current memory usage: {self._get_memory_usage()}")

        self.logger.info(f"Worker-{worker_id} exiting.")

    def _get_memory_usage(self) -> str:
        process = psutil.Process(os.getpid())
        mem = process.memory_info()
        return f"RSS: {mem.rss / 1024 / 1024:.2f}MB, VMS: {mem.vms / 1024 / 1024:.2f}MB"

    def _log_progress(self) -> None:
        """Log progress assuming 450k total for demonstration."""
        progress = (self.email_processor.processed_count / 450_000) * 100
        self.logger.info(
            f"Progress: {self.email_processor.processed_count}/450000 " 
            f"({progress:.1f}%)"
        )

    def process_emails(self) -> None:
        """
        Main method:
         1) cProfile start
         2) start one reader thread,
         3) start multiple worker threads,
         4) wait for them to finish,
         5) cProfile stop & save
        """
        profiler = cProfile.Profile()
        profiler.enable()

        try:
            self.logger.info(f"Initial memory usage => {self._get_memory_usage()}")
            # Start one reader thread
            rt = threading.Thread(target=self._reader_thread, name="ReaderThread", daemon=True)
            rt.start()

            # Start worker threads
            for wid in range(MAX_WORKERS):
                t = threading.Thread(target=self._worker_thread, args=(wid,), daemon=True)
                self.workers.append(t)
                t.start()

            # Wait for the reader to finish
            rt.join()

            # Wait for chunk queue to empty
            self.chunk_queue.join()

            # Stop all workers
            self.stop_flag = True
            for w in self.workers:
                w.join()

            self.logger.info("All worker threads joined. Processing complete.")

        except Exception as e:
            self.logger.error(f"Error processing emails: {e}")
            raise

        finally:
            profiler.disable()
            stats = pstats.Stats(profiler).sort_stats("cumulative")
            stats.dump_stats("email_indexing_profile.prof")
            self.logger.info("Profile data saved to email_indexing_profile.prof")

    def run(self) -> None:
        """Main entry point with environment validation, ML init, and parallel processing."""
        try:
            self.logger.info("Starting indexing process with queue-based reading & processing...")
            self._validate_environment()
            self.ml_models.initialize()
            self.process_emails()
            self.logger.info("Email indexing process completed successfully.")
        except Exception as e:
            self.logger.error(f"Fatal error in indexing process: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def __del__(self):
        self.cleanup()


def main():
    """Main script entry point."""
    try:
        load_dotenv()
        mbox_path = "../data/mbox_export.mbox"

        with EmailIndexer(mbox_path) as indexer:
            indexer.run()

    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
