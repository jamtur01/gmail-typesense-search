#!/usr/bin/env python3
"""
Optimized Email Indexing System for Large Mbox Files (17GB+)

Key Features:
- Multiprocessing email parsing
- Machine Learning Enrichment (NER, Embeddings, Keywords)
- Vector Database Indexing
- Robust Error Handling
- Memory-Aware Processing
"""

import os
import io
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
import multiprocessing
import queue 
import multiprocessing.queues
import ctypes
import signal
import traceback
import hashlib
from multiprocessing import Queue as MPQueue
from multiprocessing import Process, Queue, Event, Value
from queue import Empty
from threading import Lock
from collections import defaultdict

from typing import (
    Dict, List, Optional, Any, Tuple, Union
)
from dataclasses import dataclass
from pathlib import Path
from functools import lru_cache, wraps
from datetime import datetime, timezone

# Third-party imports
import orjson
import torch
import numpy as np
import spacy
from sentence_transformers import SentenceTransformer
from keybert import KeyBERT
from dotenv import load_dotenv

# Weaviate imports
import weaviate
from weaviate.classes.config import Configure, Property, DataType
from logging.handlers import RotatingFileHandler

# Configuration Constants
MAX_BODY_LENGTH = 100_000  # Maximum length of email body to process
BATCH_SIZE = 16  # Batch size for processing
EMBEDDING_BATCH_SIZE = 8  # Batch size for embeddings
CACHE_SIZE = 1000  # Number of parsed emails to keep in cache
LOG_INTERVAL = 100  # How often to log progress
GC_INTERVAL = 500  # Frequency of garbage collection
CHUNK_MEMORY_LIMIT = 2 * 1024 * 1024 * 1024  # 2GB memory limit per process

# Process settings
MAX_WORKERS = min(4, os.cpu_count() or 1)
QUEUE_MAX_SIZE = MAX_WORKERS * 2
NLP_PROCESSES = min(2, os.cpu_count() or 1)
TORCH_THREADS = min(4, os.cpu_count() or 1)

def configure_environment():
    """Configure runtime environment for consistent, reproducible processing."""
    # Ensure consistent multiprocessing method
    if hasattr(multiprocessing, 'set_start_method'):
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass
    
    # Set random seeds for reproducibility
    import random
    random.seed(42)
    np.random.seed(42)
    torch.manual_seed(42)

def configure_threading():
    """Configure threading and parallel processing settings."""
    try:
        print("Configuring threading settings...")
        
        if hasattr(torch, 'set_num_interop_threads'):
            torch.set_num_interop_threads(2)
            print("Set torch interop threads to 2")
        
        torch.set_num_threads(TORCH_THREADS)
        print(f"Set torch threads to {TORCH_THREADS}")
        
        os.environ['MKL_NUM_THREADS'] = str(TORCH_THREADS)
        os.environ['OMP_NUM_THREADS'] = str(TORCH_THREADS)
        print(f"Set MKL and OMP threads to {TORCH_THREADS}")
        
        np.ones(1, dtype=np.float32)
        print("Initialized numpy threading")
    except Exception as e:
        print(f"Error configuring threading: {e}")

def configure_interrupt_handling():
    """
    Configure robust interrupt handling for multiprocessing.
    """
    def handle_interrupt(signum, frame):
        print(f"Received signal {signum}. Initiating graceful shutdown...")
        # Terminate all child processes
        for p in multiprocessing.active_children():
            p.terminate()
            p.join(timeout=5)
        
        # Exit the main process
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

def retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    Decorator for retrying a function with exponential backoff.
    
    :param max_attempts: Maximum number of retry attempts
    :param delay: Initial delay between retries
    :param backoff: Multiplier for delay between retries
    :param exceptions: Tuple of exceptions to catch
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt == max_attempts:
                        raise
                    
                    print(f"Attempt {attempt} failed: {e}. Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

def profile_method(func):
    def wrapper(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        result = func(*args, **kwargs)
        pr.disable()
        
        # Capture profiling output
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats(20)
        
        if hasattr(args[0], 'logger'):
            args[0].logger.info(f"Performance Profile for {func.__name__}:\n{s.getvalue()}")
        
        return result
    return wrapper

@dataclass
class EmailMetadata:
    """Structured representation of email metadata."""
    message_id: str
    subject: str
    sender: str
    recipients: str
    date_val: int
    thread_id: str
    labels: List[str]

@dataclass
class EmailEnrichments:
    """Structured representation of email enrichments."""
    body_vector: List[float]
    entities: List[Dict[str, Any]]
    entity_topics: Dict[str, List[str]]
    keywords: List[Dict[str, float]]
    importance_scores: Dict[str, float]

class StopMarker:
    """Sentinel object to signal worker threads to stop."""
    pass

STOP_MARKER = StopMarker()

class MemoryMonitor:
    """Monitor and manage memory usage during processing."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.process = psutil.Process()
        self.last_memory = self.get_memory_usage()
        self.memory_limit = CHUNK_MEMORY_LIMIT
        
    def get_memory_usage(self) -> int:
        """Get current memory usage in bytes."""
        return self.process.memory_info().rss
        
    def check_memory(self, force_gc: bool = False) -> None:
        """Check and log memory usage."""
        current_memory = self.get_memory_usage()
        memory_diff = current_memory - self.last_memory
        
        current_mb = current_memory / 1024 / 1024
        diff_mb = memory_diff / 1024 / 1024
        
        if abs(memory_diff) > 100 * 1024 * 1024 or force_gc:
            self.logger.info(
                f"Memory usage: {current_mb:.1f}MB "
                f"(Δ{diff_mb:+.1f}MB)"
            )
            if force_gc:
                gc.collect()
                new_memory = self.get_memory_usage()
                new_mb = new_memory / 1024 / 1024
                freed_mb = (current_memory - new_memory) / 1024 / 1024
                self.logger.info(
                    f"After GC: {new_mb:.1f}MB "
                    f"(Freed {freed_mb:.1f}MB)"
                )
            
        self.last_memory = current_memory

    def check_chunk_size(self, chunk_size: int) -> bool:
        """Determine if processing another chunk is safe."""
        current_memory = self.get_memory_usage()
        estimated_needed = chunk_size * 100 * 1024  # Assume 100KB per email
        total_estimated = current_memory + estimated_needed
        
        can_process = total_estimated < self.memory_limit
        
        if not can_process:
            current_mb = current_memory / 1024 / 1024
            limit_mb = self.memory_limit / 1024 / 1024
            estimated_mb = estimated_needed / 1024 / 1024
            self.logger.warning(
                f"Memory limit check: current={current_mb:.1f}MB, "
                f"needed={estimated_mb:.1f}MB, limit={limit_mb:.1f}MB"
            )
            
            # Try garbage collection
            gc.collect()
            new_memory = self.get_memory_usage()
            if new_memory < current_memory:
                freed_mb = (current_memory - new_memory) / 1024 / 1024
                self.logger.info(f"GC freed {freed_mb:.1f}MB")
                return (new_memory + estimated_needed) < self.memory_limit
                
        return can_process

class MLModels:
    """Manage Machine Learning models for email processing."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.temp_dirs = []
        atexit.register(self.cleanup_temp_directories)
        
        # ML model placeholders
        self.embedding_model: Optional[SentenceTransformer] = None
        self.nlp: Optional[Any] = None
        self.keybert: Optional[KeyBERT] = None

    def initialize(self) -> None:
        """Initialize all ML models with CPU optimizations."""
        try:
            self._initialize_embedding_model()
            self._initialize_nlp()
            self._initialize_keybert()
            self.logger.info("All ML models initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize ML models: {e}")
            raise

    def _initialize_embedding_model(self) -> None:
        """Initialize embedding model optimized for CPU."""
        gc.collect()
        
        self.logger.info("Loading SentenceTransformer for CPU")
        model_name = "all-MiniLM-L6-v2"  # Smaller, CPU-efficient model
        
        # Use device config to load on CPU
        device = torch.device('cpu')
        self.embedding_model = SentenceTransformer(model_name, device=device)
        
        # Optimize model for inference
        self.embedding_model.eval()
        self.embedding_model.max_seq_length = 256

        # Quick model validation
        test_embedding = self.embedding_model.encode(
            ["Test text"],
            batch_size=1,
            show_progress_bar=False,
            normalize_embeddings=True
        )
        
        embedding_size = len(test_embedding[0])
        self.logger.info(f"Embedding model initialized (dim={embedding_size})")

    def _initialize_nlp(self) -> None:
        """Initialize spaCy with comprehensive settings."""
        try:
            # Download the model if not already present
            try:
                self.nlp = spacy.load("en_core_web_sm")
            except OSError:
                self.logger.warning("spaCy model not found; downloading...")
                os.system("python -m spacy download en_core_web_sm")
                self.nlp = spacy.load("en_core_web_sm")
            
            # Disable unnecessary pipeline components
            disabled = ['parser', 'senter']
            for name in disabled:
                if name in self.nlp.pipe_names:
                    self.nlp.remove_pipe(name)
            
            # Ensure essential components are present
            if 'tagger' not in self.nlp.pipe_names:
                try:
                    self.nlp.add_pipe('tagger', last=True)
                except Exception as e:
                    self.logger.warning(f"Could not add tagger: {e}")
            
            self.logger.info(f"Active pipeline components: {self.nlp.pipe_names}")
            self.logger.info("spaCy model loaded successfully.")
        
        except Exception as e:
            self.logger.error(f"Failed to initialize spaCy NLP: {e}")
            raise

    def _initialize_keybert(self) -> None:
        """Initialize KeyBERT using embedding model."""
        self.logger.info("Initializing KeyBERT...")
        self.keybert = KeyBERT(model=self.embedding_model)

    def extract_keywords(self, text: str) -> List[Dict[str, float]]:
        """
        Enhanced keyword extraction with scores and diversity.
        
        Returns:
        List of dicts with {'word': keyword, 'score': score}
        """
        try:
            # Use MMR for diverse keyword selection
            keywords = self.keybert.extract_keywords(
                text,
                keyphrase_ngram_range=(1, 2),  # Allow both single words and bigrams
                stop_words='english',  # Filter common English stop words
                top_n=10,  # Get more keywords
                use_maxsum=True,
                use_mmr=True,
                diversity=0.7
            )
            
            # Convert to list of dicts with normalized scores
            return [
                {
                    "word": word,
                    "score": float(score),
                    "ngram_size": len(word.split())
                }
                for word, score in keywords
            ]
        except Exception as e:
            self.logger.error(f"Keyword extraction error: {e}")
            return []

    def extract_entity_topics(self, entities: List[Dict]) -> Dict[str, List[str]]:
        """Group entities by topics"""
        topics = {
            "TECH": ["PRODUCT", "TECHNOLOGY", "SOFTWARE"],
            "BUSINESS": ["ORG", "COMPANY", "MONEY"],
            "PEOPLE": ["PERSON", "PER"],
            "LOCATION": ["GPE", "LOC"],
            "TIME": ["DATE", "TIME"]
        }
        
        categorized = defaultdict(list)
        for entity in entities:
            for topic, labels in topics.items():
                if entity["label"] in labels:
                    categorized[topic].append(entity)
        
        return dict(categorized)

    def classify_entity_importance(self, ent, doc) -> float:
        """Score entity importance"""
        importance = 1.0
        
        # Location bonus
        if ent.start < len(doc) * 0.2:  # Entity appears in first 20% of text
            importance *= 1.2
        
        # Frequency bonus
        frequency = sum(1 for e in doc.ents if e.text == ent.text)
        importance *= (1 + (frequency - 1) * 0.1)
        
        # Title case bonus
        if ent.text.istitle():
            importance *= 1.1
        
        # Length bonus
        if len(ent.text.split()) > 1:  # Multi-word entity
            importance *= 1.1
        
        return importance

    def process_entities(self, doc) -> List[Dict[str, Any]]:
        """Enhanced entity processing"""
        entities = []
        for ent in doc.ents:
            entity = {
                "text": ent.text,
                "label": ent.label_,
                "start": ent.start_char,
                "end": ent.end_char,
                "description": spacy.explain(ent.label_),  # Get entity type explanation
                "context": doc[max(0, ent.start-5):min(len(doc), ent.end+5)].text,  # Surrounding context
                "root_text": ent.root.text,  # Head token of entity
                "dependencies": [
                    {"text": token.text, "dep": token.dep_}
                    for token in ent.root.children
                ]
            }
            entities.append(entity)
        return entities

    def process_texts_parallel(self, texts: List[str], batch_size: int = 32) -> Tuple[np.ndarray, List[List[Dict]], List[List[str]]]:
        """
        Robust text processing with extensive error handling.
        """
        if not texts:
            return np.array([]), [], []
        
        # Truncate texts to max sequence length and clean
        truncated_texts = [
            text[:self.embedding_model.max_seq_length].strip() 
            for text in texts
        ]
        
        # Remove empty texts
        valid_texts = [t for t in truncated_texts if t]
        
        try:
            # Get embeddings with fallback
            try:
                embeddings = self.encode_batch(valid_texts, batch_size)
            except Exception as emb_err:
                self.logger.error(f"Embedding error: {emb_err}")
                embeddings = np.zeros((len(valid_texts), self.embedding_model.get_sentence_embedding_dimension()))
            
            # Process NER with fallback
            try:
                batch_docs = list(self.nlp.pipe(valid_texts, batch_size=batch_size))
                entities = [[{'text': ent.text, 'label': ent.label_} 
                           for ent in doc.ents] for doc in batch_docs]
            except Exception as ner_err:
                self.logger.error(f"NER processing error: {ner_err}")
                entities = [[] for _ in valid_texts]
            
            # Extract keywords with fallback
            try:
                keywords = []
                for text in valid_texts:
                    try:
                        text_keywords = self.extract_keywords(text)
                        keywords.append(text_keywords)
                    except Exception as kw_err:
                        self.logger.error(f"Keyword extraction error: {kw_err}")
                        keywords.append([])
            except Exception as kw_gen_err:
                    self.logger.error(f"Keywords generation error: {kw_gen_err}")
                    keywords = [[] for _ in valid_texts]
    
            return embeddings, entities, keywords
        
        except Exception as e:
            self.logger.error(f"Comprehensive text processing error: {e}")
            return (
                np.zeros((len(valid_texts), self.embedding_model.get_sentence_embedding_dimension())), 
                [[] for _ in valid_texts], 
                [[] for _ in valid_texts]
            )

    def encode_batch(self, texts: List[str], batch_size: int = 32) -> np.ndarray:
        """Optimized batch encoding for CPU."""
        with torch.no_grad():
            try:
                embeddings = []
                for i in range(0, len(texts), batch_size):
                    batch = texts[i:i + batch_size]
                    batch_embeddings = self.embedding_model.encode(
                        batch,
                        batch_size=batch_size,
                        show_progress_bar=False,
                        normalize_embeddings=True,
                        convert_to_numpy=True
                    )
                    embeddings.append(batch_embeddings)
                
                return np.vstack(embeddings)
                
            except Exception as e:
                self.logger.error(f"Error in batch encoding: {e}")
                return np.zeros((len(texts), self.embedding_model.get_sentence_embedding_dimension()))

    def create_temp_directory(self) -> str:
        """Create and track temporary directory."""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def cleanup_temp_directories(self) -> None:
        """Clean up temporary directories."""
        for temp_dir in self.temp_dirs:
            try:
                os.rmdir(temp_dir)
                self.logger.debug(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up temp dir: {temp_dir} => {e}")

    def cleanup(self) -> None:
        """Clean up ML models and free memory."""
        for attr in ['embedding_model', 'nlp', 'keybert']:
            if hasattr(self, attr) and getattr(self, attr) is not None:
                delattr(self, attr)
        
        gc.collect()
        self.logger.info("ML models cleaned up and memory freed.")
        self.cleanup_temp_directories()

class WeaviateIndexer:
    """Handles Weaviate client connection, schema creation, and object insertion."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.client = None
        self.collection_name = "EmailIndex"
        
    @retry(max_attempts=3, exceptions=(ConnectionError, RuntimeError))
    def connect(self) -> None:
        """Establish connection to Weaviate with retry mechanism."""
        try:
            self.client = weaviate.connect_to_local()
            
            if not self.client.is_ready():
                raise ConnectionError("Weaviate instance is not ready")
            
            self.logger.info("Connected to Weaviate successfully")
            self._create_schema()
        except Exception as e:
            self.logger.error(f"Weaviate connection failed: {e}")
            raise

    def _create_schema(self) -> None:
        """Create the Email schema with enhanced entity properties."""
        try:
            # Check if collection exists
            if not self.client.collections.exists("Email"):
                self.client.collections.create(
                    name="Email",
                    vectorizer_config=None,  # We'll provide vectors ourselves
                    properties=[
                        Property(
                            name="message_id",
                            data_type=DataType.TEXT,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        Property(
                            name="subject",
                            data_type=DataType.TEXT,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        Property(
                            name="sender",
                            data_type=DataType.TEXT,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        Property(
                            name="recipients",
                            data_type=DataType.TEXT,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        Property(
                            name="date_val",
                            data_type=DataType.DATE,
                            indexFilterable=True,
                            indexRangeFilters=True
                        ),
                        Property(
                            name="thread_id",
                            data_type=DataType.TEXT,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        Property(
                            name="labels",
                            data_type=DataType.TEXT_ARRAY,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        Property(
                            name="body",
                            data_type=DataType.TEXT,
                            indexFilterable=True,
                            indexSearchable=True
                        ),
                        # Enhanced entities with additional properties
                        Property(
                            name="entities", 
                            data_type=DataType.OBJECT_ARRAY,
                            indexFilterable=True,
                            nested_properties=[
                                Property(
                                    name="text",
                                    data_type=DataType.TEXT,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="label",
                                    data_type=DataType.TEXT,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="start",
                                    data_type=DataType.NUMBER,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                ),
                                Property(
                                    name="end",
                                    data_type=DataType.NUMBER,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                ),
                                Property(
                                    name="description",
                                    data_type=DataType.TEXT,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="context",
                                    data_type=DataType.TEXT,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="importance",
                                    data_type=DataType.NUMBER,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                )
                            ]
                        ),
                        # Topic classification
                        Property(
                            name="entity_topics",
                            data_type=DataType.OBJECT,
                            nestedProperties=[
                                Property(
                                    name="TECH",
                                    data_type=DataType.TEXT_ARRAY,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="BUSINESS",
                                    data_type=DataType.TEXT_ARRAY,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="PEOPLE",
                                    data_type=DataType.TEXT_ARRAY,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="LOCATION",
                                    data_type=DataType.TEXT_ARRAY,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="TIME",
                                    data_type=DataType.TEXT_ARRAY,
                                    indexFilterable=True,
                                    indexSearchable=True
                                )
                            ]
                        ),
                        # Entity importance scores
                        Property(
                            name="entity_importance",
                            data_type=DataType.OBJECT,
                            indexFilterable=True,
                            nestedProperties=[
                                Property(
                                    name="score",
                                    data_type=DataType.NUMBER,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                ),
                                Property(
                                    name="entity",
                                    data_type=DataType.TEXT,
                                    indexFilterable=True,
                                    indexSearchable=True
                                )
                            ]
                        ),
                        # Enhanced keywords
                        Property(
                            name="keywords",
                            data_type=DataType.OBJECT_ARRAY,
                            indexFilterable=True,
                            nested_properties=[
                                Property(
                                    name="word",
                                    data_type=DataType.TEXT,
                                    indexFilterable=True,
                                    indexSearchable=True
                                ),
                                Property(
                                    name="score",
                                    data_type=DataType.NUMBER,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                ),
                                Property(
                                    name="ngram_size",
                                    data_type=DataType.NUMBER,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                )
                            ]
                        )
                    ]
                )
                self.logger.info("Email schema created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating Email schema: {e}")
            raise
         
    def _convert_timestamp_to_iso(self, timestamp: int) -> str:
        """Convert Unix timestamp to ISO format with UTC timezone."""
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

    @retry(max_attempts=3)
    def insert_objects_batch(self, email_objs: List[Dict[str, Any]]) -> None:
        """
        Optimized batch insertion with multiple retry strategies.
        """
        if not email_objs:
            self.logger.warning("No objects to insert")
            return
        
        try:
            collection = self.client.collections.get("Email")
            
            # Use direct insertion method
            for email_obj in email_objs:
                try:
                    keywords_data = email_obj.get("keywords", [])
                    keywords_with_scores = [
                        {
                            "word": kw["word"],
                            "score": kw["score"],
                            "ngram_size": kw.get("ngram_size", 1)
                        }
                        for kw in keywords_data
                    ]
                    obj_data = {
                        "message_id": email_obj.get("message_id", str(uuid.uuid4())),
                        "subject": email_obj.get("subject", ""),
                        "sender": email_obj.get("sender", ""),
                        "recipients": email_obj.get("recipients", ""),
                        "date_val": self._convert_timestamp_to_iso(
                            email_obj.get("date_val", int(time.time()))
                        ),
                        "thread_id": email_obj.get("thread_id", ""),
                        "labels": email_obj.get("labels", []),
                        "body": email_obj.get("body", "")[:MAX_BODY_LENGTH],
                        "entities": [
                            {"text": e.get('text', ''), "label": e.get('label', '')} 
                            for e in email_obj.get("entities", [])
                        ],
                        "keywords": keywords_with_scores
                    }
                    
                    # Insert with vector
                    collection.data.insert(
                        properties=obj_data,
                        vector=email_obj.get("body_vector", [0]*384)
                    )
                except Exception as obj_err:
                    self.logger.error(
                        f"Failed to add object {email_obj.get('message_id', 'UNKNOWN')}: {obj_err}"
                    )
                    
        except Exception as e:
            self.logger.error(f"Batch insertion failed: {e}")
            raise

    def close_connection(self) -> None:
        """Safely close Weaviate connection."""
        try:
            if self.client:
                self.client.close()
                self.logger.info("Weaviate client connection closed")
        except Exception as e:
            self.logger.error(f"Error closing Weaviate connection: {e}")

class EmailProcessor:
    """
    Comprehensive email processing with enrichment and indexing.
    
    Handles:
    - Email parsing
    - Metadata extraction
    - ML-based enrichment
    - Checkpoint management
    """

    def __init__(self, logger: logging.Logger, ml_models: MLModels, weaviate_indexer: WeaviateIndexer):
        self.logger = logger
        self.ml_models = ml_models
        self.weaviate_indexer = weaviate_indexer
        
        # Checkpoint management
        self.checkpoint_file = Path("email_indexing_checkpoint.json")
        self.processed_count = self._load_checkpoint()
        
        # Parsing cache
        self.parsed_cache: Dict[str, Any] = {}

    def batch_parse_raw_messages(self, raw_msgs: List[str]) -> Tuple[List[Optional[EmailMetadata]], List[str]]:
        """
        Parse raw messages using cached method.
        
        :param raw_msgs: List of raw email messages
        :return: Tuple of (metadata list, body list)
        """
        meta_list: List[Optional[EmailMetadata]] = []
        body_list: List[str] = []

        for raw in raw_msgs:
            meta, body = self._parse_email_cached(raw)
            meta_list.append(meta)
            body_list.append(body)
        
        return meta_list, body_list

    @profile_method
    def batch_enrich(self, meta_list: List[Optional[EmailMetadata]], body_list: List[str]) -> List[Dict[str, Any]]:
        """
        Robust batch enrichment with comprehensive error handling and enhanced entity processing.
        
        :param meta_list: List of email metadata
        :param body_list: List of email bodies
        :return: List of enriched email objects
        """

        start_time = time.time()

        try:
            # Prepare input texts
            text_process_start = time.time()
            texts = [
                body[:self.ml_models.embedding_model.max_seq_length].strip() 
                for body in body_list
            ]
            
            # Remove empty texts and track their original indices
            valid_indices = [i for i, text in enumerate(texts) if text]
            valid_texts = [texts[i] for i in valid_indices]
            
            # If no valid texts, return empty list
            if not valid_texts:
                return []
            
            try:
                # Process texts in parallel for embeddings and keywords
                embeddings, base_entities, keywords = self.ml_models.process_texts_parallel(
                    valid_texts,
                    batch_size=EMBEDDING_BATCH_SIZE
                )
            except Exception as process_err:
                self.logger.error(f"Text processing error: {process_err}")
                # Fallback: create empty enrichments
                embeddings = np.zeros((len(valid_texts), 384))
                base_entities = [[] for _ in valid_texts]
                keywords = [[] for _ in valid_texts]
            
            text_process_time = time.time() - text_process_start

            # Combine enrichments with metadata
            ner_start = time.time()
            enriched_emails = []
            valid_result_idx = 0
            
            for idx, (meta, body) in enumerate(zip(meta_list, body_list)):
                # Skip if no metadata or empty body
                if not meta or not body.strip():
                    continue
                
                # Check if this index was in valid texts
                if idx in valid_indices:
                    try:
                        # Process document with enhanced NER
                        doc_process_start = time.time()
                        doc = self.ml_models.nlp(body[:MAX_BODY_LENGTH])
                        doc_process_time = time.time() - doc_process_start
                        
                        # Get enhanced entities
                        entity_start = time.time()
                        enhanced_entities = self.ml_models.process_entities(doc)
                        entity_process_time = time.time() - entity_start

                        # Get entity topics
                        topics_start = time.time()
                        entity_topics = self.ml_models.extract_entity_topics(enhanced_entities)
                        topics_time = time.time() - topics_start

                        # Calculate importance scores
                        importance_start = time.time()
                        entity_importance = {
                            ent["text"]: self.ml_models.classify_entity_importance(
                                doc[ent["start"]:ent["end"]], 
                                doc
                            )
                            for ent in enhanced_entities
                        }
                        importance_time = time.time() - importance_start

                        email_dict = {
                            "message_id": meta.message_id,
                            "subject": meta.subject,
                            "sender": meta.sender,
                            "recipients": meta.recipients,
                            "date_val": meta.date_val,
                            "thread_id": meta.thread_id,
                            "labels": meta.labels,
                            "body": body[:MAX_BODY_LENGTH],
                            "body_vector": embeddings[valid_result_idx].tolist(),
                            "entities": enhanced_entities,
                            "entity_topics": entity_topics,
                            "entity_importance": entity_importance,
                            "keywords": keywords[valid_result_idx]
                        }
                        
                        valid_result_idx += 1
                        enriched_emails.append(email_dict)
                        
                        self.logger.debug(
                            f"Email processing times: "
                            f"Doc: {doc_process_time:.4f}s, "
                            f"Entities: {entity_process_time:.4f}s, "
                            f"Topics: {topics_time:.4f}s, "
                            f"Importance: {importance_time:.4f}s"
                        )

                    except IndexError as ie:
                        # Fallback for index errors
                        self.logger.error(f"Index error in enrichment: {ie}")
                        email_dict = {
                            "message_id": meta.message_id,
                            "subject": meta.subject,
                            "sender": meta.sender,
                            "recipients": meta.recipients,
                            "date_val": meta.date_val,
                            "thread_id": meta.thread_id,
                            "labels": meta.labels,
                            "body": body[:MAX_BODY_LENGTH],
                            "body_vector": [0.0] * 384,
                            "entities": [],
                            "entity_topics": {},
                            "entity_importance": {},
                            "keywords": []
                        }
                        enriched_emails.append(email_dict)
            
            total_time = time.time() - start_time
            self.logger.info(
                f"Batch Enrichment Performance: "
                f"Total Time: {total_time:.2f}s, "
                f"Text Processing: {text_process_time:.2f}s, "
                f"NER Processing: {time.time() - ner_start:.2f}s, "
                f"Processed Emails: {len(enriched_emails)}"
            )

            return enriched_emails

        except Exception as e:
            self.logger.error(f"Comprehensive batch enrichment error: {e}")
            return []


    def process_chunk(self, raw_msgs: List[str]) -> None:
        """
        Process a chunk of raw email messages.
        
        :param raw_msgs: List of raw email messages
        """
        try:
            # Parse messages
            meta_list, body_list = self.batch_parse_raw_messages(raw_msgs)

            # Periodically clear cache
            if self.processed_count % 1000 == 0:
                self._parse_email_cached.cache_clear()
                gc.collect()

            # Enrich messages
            enriched = self.batch_enrich(meta_list, body_list)

            # Insert into Weaviate
            if enriched:
                self.weaviate_indexer.insert_objects_batch(enriched)

            # Update progress
            self.processed_count += len(enriched)
            self._save_checkpoint()
            
            if self.processed_count % LOG_INTERVAL == 0:
                self.logger.info(
                    f"Processed+Inserted {len(enriched)} emails; "
                    f"total: {self.processed_count}"
                )

        except Exception as e:
            self.logger.error(f"Chunk processing error: {e}")
            traceback.print_exc()
        finally:
            # Clear references and run garbage collection
            del meta_list
            del body_list
            gc.collect()

    def _load_checkpoint(self) -> int:
        """
        Load processing checkpoint with robust error handling.
        
        :return: Last processed index
        """
        try:
            if self.checkpoint_file.exists():
                data = orjson.loads(self.checkpoint_file.read_bytes())
                last_processed = data.get("last_processed_index", 0)
                self.logger.info(f"Loaded checkpoint: {last_processed}")
                return last_processed
            return 0
        except Exception as e:
            self.logger.error(f"Checkpoint loading failed: {e}")
            return 0

    def _save_checkpoint(self) -> None:
        """
        Save processing checkpoint with atomic write.
        """
        try:
            temp_checkpoint = self.checkpoint_file.with_suffix('.tmp')
            temp_checkpoint.write_bytes(
                orjson.dumps({
                    "last_processed_index": self.processed_count,
                    "timestamp": datetime.now().isoformat()
                })
            )
            # Atomic replace
            temp_checkpoint.replace(self.checkpoint_file)
            
            self.logger.debug(f"Checkpoint saved: {self.processed_count}")
        except Exception as e:
            self.logger.error(f"Checkpoint save failed: {e}")

    @lru_cache(maxsize=CACHE_SIZE)
    def _parse_email_cached(self, raw_msg: str) -> Tuple[Optional[EmailMetadata], str]:
        """
        Cached email parsing with robust encoding handling.
        
        :param raw_msg: Raw email message string
        :return: Tuple of (metadata, body)
        """
        try:
            # Generate stable hash for caching
            msg_hash = hashlib.md5(raw_msg.encode('utf-8', errors='ignore')).hexdigest()
            
            if msg_hash in self.parsed_cache:
                return self.parsed_cache[msg_hash]
            
            # Attempt to handle different encoding scenarios
            try:
                # Try decoding with various error handling strategies
                if isinstance(raw_msg, bytes):
                    raw_msg = self._decode_bytes(raw_msg)
            except Exception as decode_err:
                self.logger.error(f"Decoding error: {decode_err}")
                return None, ""
            
            msg_obj = self.parse_email_single(raw_msg)
            if not msg_obj:
                return None, ""
            
            meta = self.extract_email_metadata(msg_obj)
            body = self.extract_email_body(msg_obj)
            
            result = (meta, body)
            self.parsed_cache[msg_hash] = result
            return result
        except Exception as e:
            self.logger.error(f"Cached parsing failed: {e}")
            return None, ""

    def _decode_bytes(self, raw_msg: bytes) -> str:
        """
        Robust method to decode bytes with multiple encoding strategies.
        
        :param raw_msg: Bytes to decode
        :return: Decoded string
        """
        # List of encodings to try, in order of preference
        encodings = [
            'utf-8',    # Most common
            'latin-1',  # Can handle all byte values
            'iso-8859-1',
            'cp1252',   # Windows encoding
            'ascii'     # Fallback
        ]
        
        # Try each encoding with different error handling
        error_strategies = [
            'strict',    # Raise error on first problematic character
            'replace',   # Replace problematic characters
            'ignore'     # Remove problematic characters
        ]
        
        for encoding in encodings:
            for strategy in error_strategies:
                try:
                    decoded = raw_msg.decode(encoding, errors=strategy)
                    
                    # Additional sanitization
                    decoded = ''.join(char for char in decoded if char.isprintable() or char.isspace())
                    
                    return decoded
                except Exception:
                    continue
        
        # Absolute fallback
        return raw_msg.decode('utf-8', errors='ignore')

    def parse_email_single(self, raw_msg: str) -> Optional[email.message.EmailMessage]:
        """
        Parse a single raw email message with robust error handling.
        
        :param raw_msg: Raw email message string
        :return: Parsed email message or None
        """
        try:
            # Sanitize input by removing or replacing problematic characters
            sanitized_msg = ''.join(
                char for char in raw_msg 
                if char.isprintable() or char.isspace()
            )
            
            return email.message_from_string(sanitized_msg)
        except Exception as e:
            self.logger.error(f"Email parsing failed: {e}")
            return None

    def extract_email_metadata(self, msg: email.message.EmailMessage) -> Optional[EmailMetadata]:
        """
        Extract structured metadata from email message with robust decoding.
        
        :param msg: Parsed email message
        :return: EmailMetadata object
        """
        try:
            def safe_decode(value: Union[str, bytes, None], fallback: str = "") -> str:
                """
                Safely decode email header values.
                
                :param value: Header value to decode
                :param fallback: Fallback value if decoding fails
                :return: Decoded string
                """
                if value is None:
                    return fallback
                
                if isinstance(value, bytes):
                    try:
                        # Try different decoding strategies
                        decoded = value.decode('utf-8', errors='replace')
                    except Exception:
                        try:
                            decoded = value.decode('latin-1', errors='replace')
                        except Exception:
                            decoded = str(value)
                    
                    return decoded
                
                return str(value)
            
            return EmailMetadata(
                message_id=safe_decode(msg.get("Message-ID"), str(uuid.uuid4())),
                subject=safe_decode(msg.get("Subject")),
                sender=safe_decode(msg.get("From")),
                recipients=safe_decode(msg.get("To")),
                date_val=int(time.time()),  # Fallback timestamp
                thread_id=safe_decode(msg.get("Thread-ID")),
                labels=[
                    safe_decode(label) 
                    for label in msg.get("X-Gmail-Labels", "").split(",") 
                    if label.strip()
                ]
            )
        except Exception as e:
            self.logger.error(f"Metadata extraction failed: {e}")
            return None

    def extract_email_body(self, msg: email.message.EmailMessage) -> str:
        """
        Extract email body with comprehensive multipart and encoding support.
        
        :param msg: Parsed email message
        :return: Extracted email body
        """
        try:
            def safe_decode_payload(payload: bytes, charset: Optional[str] = None) -> str:
                """
                Safely decode email payload with multiple encoding strategies.
                
                :param payload: Payload bytes
                :param charset: Suggested charset
                :return: Decoded string
                """
                if not payload:
                    return ""
                
                # List of encodings to try
                encodings = [
                    charset,  # Try suggested charset first
                    'utf-8', 
                    'latin-1', 
                    'iso-8859-1', 
                    'cp1252'
                ]
                
                # Remove None from encodings
                encodings = [enc for enc in encodings if enc]
                
                for encoding in encodings:
                    try:
                        decoded = payload.decode(encoding, errors='replace')
                        # Remove non-printable characters
                        return ''.join(
                            char for char in decoded 
                            if char.isprintable() or char.isspace()
                        )
                    except Exception:
                        continue
                
                # Absolute fallback
                return payload.decode('utf-8', errors='ignore')
            
            if msg.is_multipart():
                body_parts = []
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/plain" and not part.get("Content-Disposition"):
                        payload = part.get_payload(decode=True)
                        charset = part.get_content_charset()
                        
                        if payload:
                            decoded_part = safe_decode_payload(payload, charset)
                            body_parts.append(decoded_part)
                
                # Join body parts, truncate if necessary
                body = "\n".join(body_parts)[:MAX_BODY_LENGTH]
            
            # Single part message
            else:
                payload = msg.get_payload(decode=True)
                charset = msg.get_content_charset()
                
                body = safe_decode_payload(payload, charset)[:MAX_BODY_LENGTH]
            
            return body.strip()
        
        except Exception as e:
            self.logger.error(f"Body extraction error: {e}")
            return ""

class ProcessPool:
    """
    Manages a pool of worker processes with advanced coordination 
    and graceful shutdown mechanisms.
    """
    
    def __init__(self, num_workers: int, mbox_path: str):
        self.num_workers = num_workers
        self.mbox_path = mbox_path
        self.workers: List[Process] = []
        self.queue = Queue(maxsize=QUEUE_MAX_SIZE)
        self.stop_event = Event()
        self.processed_count = Value(ctypes.c_int, 0)
        self.error_count = Value(ctypes.c_int, 0)
        
        # Signal handling
        self._original_sigint_handler = signal.signal(signal.SIGINT, self._handle_signal)
        self._original_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        """
        Gracefully handle termination signals.
        
        :param signum: Signal number
        :param frame: Current stack frame
        """
        print(f"Received signal {signum}. Initiating graceful shutdown...")
        self.stop()
    
    def stop(self):
        """
        Comprehensive method to stop all workers and clean up resources.
        """
        try:
            # Signal workers to stop
            self.stop_event.set()
            
            # Put stop markers for each worker
            for _ in range(self.num_workers):
                try:
                    self.queue.put(STOP_MARKER, block=False)
                except Exception:
                    pass
            
            # Wait for and terminate workers
            for worker in self.workers:
                worker.join(timeout=10)
                
                # Force terminate if not responding
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
            
            # Close the queue
            self.queue.close()
            self.queue.join_thread()
            
            # Restore original signal handlers
            signal.signal(signal.SIGINT, self._original_sigint_handler)
            signal.signal(signal.SIGTERM, self._original_sigterm_handler)
            
        except Exception as e:
            print(f"Error during shutdown: {e}")
    
    def shutdown(self):
        """
        Comprehensive shutdown method.
        """
        try:
            # Set stop event to signal workers to stop
            self.stop_event.set()
            
            # Put stop markers for each worker
            for _ in range(self.num_workers):
                try:
                    self.queue.put(STOP_MARKER, block=False)
                except Exception:
                    pass
            
            # Wait for workers to finish
            for worker in self.workers:
                worker.join(timeout=10)
                
                # Force terminate if not responding
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
            
            # Close the queue
            self.queue.close()
            self.queue.join_thread()
            
            # Restore original signal handlers
            signal.signal(signal.SIGINT, self._original_sigint_handler)
            signal.signal(signal.SIGTERM, self._original_sigterm_handler)
            
        except Exception as e:
            print(f"Error during shutdown: {e}")
    
    def start_workers(self) -> None:
        """
        Start worker processes for email processing.
        """
        for i in range(self.num_workers):
            p = Process(
                target=self._worker_process_init,
                args=(
                    i, 
                    self.mbox_path, 
                    self.queue, 
                    self.stop_event, 
                    self.processed_count, 
                    self.error_count
                )
            )
            p.daemon = False  # Non-daemonic for proper cleanup
            p.start()
            self.workers.append(p)
            print(f"Started worker {i}")
    
    @staticmethod
    def _worker_process_init(worker_id: int, mbox_path: str, 
                            queue: MPQueue, stop_event: Event,
                            processed_count: Value, error_count: Value) -> None:
        """
        Static method to initialize and run a worker process.
        """
        # Setup process-specific logger
        logger = logging.getLogger(f"Worker-{worker_id}")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
        
        # Initialize components
        ml_models = None
        weaviate_indexer = None
        email_processor = None
        
        try:
            # Initialize worker-specific components
            ml_models = MLModels(logger)
            ml_models.initialize()
            
            weaviate_indexer = WeaviateIndexer(logger)
            weaviate_indexer.connect()
            
            email_processor = EmailProcessor(logger, ml_models, weaviate_indexer)
            memory_monitor = MemoryMonitor(logger)

            logger.info(f"Worker {worker_id} initialized, starting processing")
            
            # Processing loop
            while not stop_event.is_set():
                try:
                    # Get chunk with timeout
                    chunk = queue.get(timeout=1)
                    
                    # Check for stop marker
                    if chunk is STOP_MARKER:
                        logger.info(f"Worker {worker_id} received STOP_MARKER")
                        break
                    
                    # Memory check before processing
                    if not memory_monitor.check_chunk_size(len(chunk)):
                        logger.warning("Memory limit would be exceeded, skipping chunk")
                        continue
                    
                    # Process chunk
                    email_processor.process_chunk(chunk)
                    
                    # Update counter
                    with processed_count.get_lock():
                        processed_count.value += len(chunk)
                    
                    # Periodic memory check
                    if processed_count.value % GC_INTERVAL == 0:
                        memory_monitor.check_memory(force_gc=True)
                        
                except multiprocessing.queues.Empty:
                    # Timeout on queue, continue waiting
                    continue
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}")
                    with error_count.get_lock():
                        error_count.value += 1
                    
        except Exception as e:
            logger.error(f"Worker {worker_id} initialization failed: {e}")
            traceback.print_exc()
        finally:
            # Comprehensive cleanup
            try:
                if email_processor:
                    del email_processor
                if ml_models:
                    ml_models.cleanup()
                    del ml_models
                if weaviate_indexer:
                    weaviate_indexer.close_connection()
                    del weaviate_indexer
                
                gc.collect()
                logger.info(f"Worker {worker_id} cleanup completed")
            except Exception as e:
                logger.error(f"Error during worker {worker_id} cleanup: {e}")

def reader_process_init(mbox_path: str, queue: Queue, stop_event: Event) -> None:
    """
    Standalone reader process to stream emails into processing queue.
    
    :param mbox_path: Path to the mbox file
    :param queue: Shared queue for email chunks
    :param stop_event: Event to signal process termination
    """
    # Setup process-specific logger
    reader_logger = logging.getLogger("Reader")
    reader_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    reader_logger.addHandler(handler)
    
    try:
        reader_logger.info(f"Reader process started, opening {mbox_path}")
        mbox = mailbox.mbox(str(mbox_path))
        batch_raw: List[str] = []

        for msg in mbox:
            if stop_event.is_set():
                break

            try:
                raw_msg = msg.as_string()
                batch_raw.append(raw_msg)

                if len(batch_raw) >= BATCH_SIZE:
                    # Put chunk into the queue
                    queue.put(batch_raw)
                    reader_logger.debug(f"Reader queued chunk of size {len(batch_raw)}")
                    batch_raw = []

            except Exception as e:
                reader_logger.error(f"Failed to process message: {e}")
                continue

        # Handle any remaining messages
        if batch_raw and not stop_event.is_set():
            queue.put(batch_raw)
            reader_logger.debug(f"Reader queued final chunk of size {len(batch_raw)}")

    except Exception as e:
        reader_logger.error(f"Reader process error: {e}")
    finally:
        # Send stop marker to all workers
        for _ in range(MAX_WORKERS):
            queue.put(STOP_MARKER)
        reader_logger.info("Reader process finished; STOP_MARKERs placed.")

class EmailIndexer:
    """
    Comprehensive email indexing system with advanced processing capabilities.
    """
    
    def __init__(self, mbox_path: str):
        # Environment and threading configuration
        configure_environment()
        configure_threading()
        
        # Path validation
        self.mbox_path = Path(mbox_path)
        if not self.mbox_path.exists():
            raise FileNotFoundError(f"MBOX file not found at {self.mbox_path}")
        
        # Logging setup
        self.logger = self._setup_logging()
        
        # Process coordination
        self.process_pool = ProcessPool(MAX_WORKERS, str(self.mbox_path))


    def __enter__(self):
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Cleanup resources when exiting the context.
        
        :param exc_type: Exception type
        :param exc_val: Exception value
        :param exc_tb: Exception traceback
        """
        self.cleanup()
        # Optionally, return False to re-raise any exceptions
        return False

    def __del__(self):
        """
        Destructor to ensure cleanup if context manager is not used.
        """
        self.cleanup()

    def _setup_logging(self) -> logging.Logger:
        """
        Configure comprehensive logging with file and console handlers.
        
        :return: Configured logger
        """
        logger = logging.getLogger("EmailIndexer")
        logger.setLevel(logging.DEBUG)

        # Handlers
        handlers = []
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            "indexing.log", 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5, 
            encoding="utf-8"
        )
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(name)s] %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        handlers.append(console_handler)

        # Add handlers
        for handler in handlers:
            logger.addHandler(handler)

        return logger
    
    def process_emails(self) -> None:
        """
        Coordinate email processing workflow.
        """
        try:
            self.logger.info("Starting email processing...")
            
            # Start worker pool
            self.process_pool.start_workers()
            
            # Start reader process
            reader = Process(
                target=reader_process_init,
                args=(
                    str(self.mbox_path), 
                    self.process_pool.queue, 
                    self.process_pool.stop_event
                )
            )
            reader.start()
            
            # Wait for completion
            reader.join()
            
            # Shutdown process pool
            self.process_pool.shutdown()
            
            # Final statistics
            self.logger.info(
                f"Processing complete. "
                f"Processed: {self.process_pool.processed_count.value}, "
                f"Errors: {self.process_pool.error_count.value}"
            )
            
        except Exception as e:
            self.logger.error(f"Error in process_emails: {e}")
            traceback.print_exc()
            self.process_pool.shutdown()
        finally:
            self.cleanup()

    def cleanup(self):
        """
        Comprehensive resource cleanup.
        """
        try:
            # Stop process pool
            if hasattr(self, 'process_pool'):
                self.process_pool.stop()
            
            # Close logger handlers
            if hasattr(self, "logger"):
                for handler in self.logger.handlers[:]:
                    handler.close()
                    self.logger.removeHandler(handler)
                    
        except Exception as e:
            print(f"Error during cleanup: {e}", file=sys.stderr)

    def run(self) -> None:
        """
        Main entry point for the indexing process.
        """
        try:
            self.logger.info("Starting indexing process...")
            self.process_emails()
            self.logger.info("Email indexing process completed successfully.")
        except Exception as e:
            self.logger.error(f"Fatal error in indexing process: {e}")
            traceback.print_exc()
            raise

def main():
    """
    Robust main function with comprehensive error handling.
    """
    try:
        # Configure interrupt handling early
        configure_interrupt_handling()
        
        # Load environment variables
        load_dotenv()
        
        # Configurable mbox path
        mbox_path = os.path.abspath("../data/mbox_export.mbox")
        
        # Validate mbox file
        if not os.path.exists(mbox_path):
            raise FileNotFoundError(f"MBOX file not found at {mbox_path}")

        # Run indexing process
        with EmailIndexer(mbox_path) as indexer:
            indexer.run()

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Performing cleanup...")
    except Exception as e:
        print(f"Fatal error during email indexing: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Ensure all child processes are terminated
        for p in multiprocessing.active_children():
            p.terminate()
            p.join(timeout=5)

if __name__ == "__main__":
    main()