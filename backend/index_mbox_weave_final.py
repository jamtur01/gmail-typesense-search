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
import gc
import sys
import time
import uuid
import email
import mailbox
import tempfile
import atexit
import logging
import psutil
import multiprocessing
import shutil
import ctypes
import signal
import traceback
import hashlib
from multiprocessing import Queue as MPQueue
from multiprocessing import Process, Event, Value
from queue import Empty
import email.policy
from concurrent.futures import ThreadPoolExecutor, as_completed
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline

from typing import (
    Dict, List, Optional, Any, Tuple, Union
)
from dataclasses import dataclass
from pathlib import Path
from functools import wraps
from datetime import datetime, timezone
import ftfy
from cachetools import cached, LRUCache

import orjson
import torch
import numpy as np
import spacy
from sentence_transformers import SentenceTransformer
from keybert import KeyBERT
from dotenv import load_dotenv
from bs4 import BeautifulSoup, SoupStrainer
import traceback

import weaviate
from weaviate.classes.config import Configure, Property, DataType
from logging.handlers import RotatingFileHandler

MAX_BODY_LENGTH = 1_000_000  # Maximum length of email body to process
BATCH_SIZE = 100  # Batch size for processing
EMBEDDING_BATCH_SIZE = 50 # Batch size for embeddings
CACHE_SIZE = 5000  # Number of parsed emails to keep in cache
CHUNK_MEMORY_LIMIT = 32 * 1024 * 1024 * 1024  # 32GB memory limit per process

# Process settings
NUM_GPUS = torch.cuda.device_count()
if NUM_GPUS == 0:
    raise EnvironmentError("No GPUs detected. Please ensure CUDA is properly installed and GPUs are available.")
MAX_WORKERS = min(NUM_GPUS, os.cpu_count() or 1)
QUEUE_MAX_SIZE = MAX_WORKERS * 2
NLP_PROCESSES = min(NUM_GPUS, os.cpu_count() or 1) 
TORCH_THREADS = min(NUM_GPUS, os.cpu_count() or 1)

def configure_environment():
    """Configure runtime environment with GPU support."""
    if hasattr(multiprocessing, 'set_start_method'):
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass
    
    import random
    random.seed(42)
    np.random.seed(42)
    torch.manual_seed(42)
    
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(42)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

def configure_threading():
    """Configure threading with GPU awareness."""
    try:
        print("Configuring threading settings...")
        
        if not torch.cuda.is_available():
            torch_threads = min(64, os.cpu_count() or 1)
            if hasattr(torch, 'set_num_interop_threads'):
                torch.set_num_interop_threads(2)
            torch.set_num_threads(torch_threads)
            os.environ['MKL_NUM_THREADS'] = str(torch_threads)
            os.environ['OMP_NUM_THREADS'] = str(torch_threads)
        
        np.ones(1, dtype=np.float32)
        print("Initialized numpy threading")
        
        if torch.cuda.is_available():
            print(f"GPU detected: {torch.cuda.get_device_name(0)}")
            print(f"GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.1f}GB")
    except Exception as e:
        print(f"Error configuring threading: {e}")

def configure_interrupt_handling():
    """
    Configure robust interrupt handling for multiprocessing.
    """
    def handle_interrupt(signum, frame):
        print(f"Received signal {signum}. Initiating graceful shutdown...")
        for p in multiprocessing.active_children():
            p.terminate()
            p.join(timeout=5)
        
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

class MemoryManager:
    def __init__(self, logger):
        self.logger = logger
        self.last_gc = time.time()
        self.gc_interval = 30

    def maybe_collect(self):
        if time.time() - self.last_gc > self.gc_interval:
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            self.last_gc = time.time()

class MemoryMonitor:
    """Monitor and manage memory usage during processing."""

    def __init__(self, logger):
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
        estimated_needed = chunk_size * 100 * 1024
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
            
            gc.collect()
            new_memory = self.get_memory_usage()
            if new_memory < current_memory:
                freed_mb = (current_memory - new_memory) / 1024 / 1024
                self.logger.info(f"GC freed {freed_mb:.1f}MB")
                return (new_memory + estimated_needed) < self.memory_limit
                
        return can_process

class MLModels:
    """Optimized Machine Learning models with enhanced GPU utilization."""
    
    def __init__(self, logger: logging.Logger, gpu_id: int):
        self.logger = logger
        self.gpu_id = gpu_id
        self.device = torch.device(f'cuda:{gpu_id}' if torch.cuda.is_available() else 'cpu')
        self.use_gpu = torch.cuda.is_available()
        if self.use_gpu:
            torch.cuda.set_device(self.device)
        
        self.temp_dirs = []
        atexit.register(self.cleanup_temp_directories)
        
        self.embedding_batch_size = 64
        self.max_sequence_length = 256
        self.spacy_batch_size = 256
        
        self.embedding_model = None
        self.ner_pipeline = None
        self.keyword_extractor = None
        self.nlp = None

        if self.use_gpu:
            torch.backends.cudnn.benchmark = True
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True
            
            self.logger.info(f"Using GPU {self.gpu_id}: {torch.cuda.get_device_name(0)}")
            self.logger.info(f"GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.1f}GB")
        else:
            self.logger.info("Using CPU for processing")
    
    def initialize(self) -> None:
        """Initialize ML models with optimized settings."""
        try:
            self._initialize_embedding_model()
            self._initialize_ner_model()
            self._initialize_keyword_extractor()
            self._initialize_spacy()
            self.logger.info(f"All ML models initialized successfully on {self.device}")
        except Exception as e:
            self.logger.error(f"Failed to initialize ML models: {e}")
            raise
    
    def _initialize_spacy(self) -> None:
        """Initialize the spaCy NLP pipeline."""
        try:
            self.logger.info("Loading spaCy model for NLP tasks...")
            self.nlp = spacy.load("en_core_web_sm")
            self.logger.info("spaCy NLP pipeline initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to load spaCy model: {e}")
            raise

    def _initialize_embedding_model(self) -> None:
        """Initialize optimized embedding model."""
        gc.collect()
        if self.use_gpu:
            torch.cuda.empty_cache()
        
        self.logger.info(f"Loading SentenceTransformer on {self.device}")
        model_name = "all-MiniLM-L12-v2"
        self.embedding_model = SentenceTransformer(model_name, device=self.device)
        
        self.embedding_model.eval()
        
        if self.use_gpu:
            try:
                self.embedding_model = self.embedding_model.half()
                self.logger.info("Converted embedding model to half precision (FP16).")
            except Exception as e:
                self.logger.warning(f"Failed to convert embedding model to half precision: {e}")
        
        self.embedding_model.max_seq_length = self.max_sequence_length
        
        with torch.inference_mode():
            test_embedding = self.embedding_model.encode(
                ["Test text"],
                batch_size=1,
                show_progress_bar=False,
                normalize_embeddings=True
            )
            embedding_size = len(test_embedding[0])
        
        self.logger.info(f"Embedding model initialized (dim={embedding_size}) on {self.device}")

    def _initialize_ner_model(self) -> None:
        """Initialize optimized NER model using Hugging Face Transformers."""
        try:
            model_name = "dbmdz/bert-large-cased-finetuned-conll03-english"
            self.logger.info(f"Loading NER model '{model_name}' on {self.device}")
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.ner_model = AutoModelForTokenClassification.from_pretrained(model_name).to(self.device)
            self.ner_pipeline = pipeline(
                "ner", 
                model=self.ner_model, 
                tokenizer=self.tokenizer,
                device=self.device.index if self.use_gpu else -1,
                aggregation_strategy="simple"
            )
            self.logger.info("Hugging Face NER model initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize NER model: {e}")
            raise

    def _initialize_keyword_extractor(self) -> None:
        """Initialize optimized keyword extractor using KeyBERT."""
        try:
            self.logger.info("Initializing KeywordExtractor...")
            self.keyword_extractor = KeyBERT(
                model='all-MiniLM-L12-v2'
            )
            self.logger.info("KeywordExtractor initialized successfully")
        except ImportError as e:
            self.logger.error(f"KeyBERT is not installed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize KeywordExtractor: {e}")
            raise

    def process_texts_parallel(self, texts: List[str], batch_size: Optional[int] = None) -> Tuple[np.ndarray, List[List[Dict]], List[List[Dict[str, float]]]]:
        """Optimized parallel text processing with concurrent inferences."""
        if not texts:
            return np.array([]), [], []
        
        batch_size = batch_size or self.embedding_batch_size
        
        truncated_texts = [
            text[:self.max_sequence_length].strip() 
            for text in texts
        ]
        
        valid_texts = [t for t in truncated_texts if t]
        
        try:
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_embeddings = executor.submit(self.encode_batch_optimized, valid_texts, batch_size)
                
                future_entities = executor.submit(self.process_ner_batch, valid_texts)
                
                future_keywords = executor.submit(self.process_keywords_batch_parallel, valid_texts, batch_size)
                
                embeddings = future_embeddings.result()
                entities = future_entities.result()
                keywords = future_keywords.result()
            
            return embeddings, entities, keywords
                
        except Exception as e:
            self.logger.error(f"Text processing error: {e}")
            return (
                np.zeros((len(valid_texts), self.embedding_model.get_sentence_embedding_dimension())),
                [[] for _ in valid_texts],
                [[] for _ in valid_texts]
            )

    def encode_batch_optimized(self, texts: List[str], batch_size: int) -> np.ndarray:
        """Optimized batch encoding with better GPU memory management."""
        with torch.inference_mode():
            try:
                if self.use_gpu:
                    torch.cuda.empty_cache()
                
                embeddings = []
                for i in range(0, len(texts), batch_size):
                    batch = texts[i:i + batch_size]
                    
                    batch_embeddings = self.embedding_model.encode(
                        batch,
                        batch_size=batch_size,
                        show_progress_bar=False,
                        normalize_embeddings=True,
                        convert_to_numpy=True,
                        device=self.device
                    )
                    embeddings.append(batch_embeddings)
                    
                    if self.use_gpu:
                        torch.cuda.empty_cache()
                
                return np.vstack(embeddings)
                
            except Exception as e:
                self.logger.error(f"Error in batch encoding: {e}")
                return np.zeros((len(texts), self.embedding_model.get_sentence_embedding_dimension()))
    
    def process_entities(self, doc) -> List[Dict[str, Any]]:
        """Enhanced entity processing with optimizations."""
        entities = []
        for ent in doc.ents:
            entity = {
                "text": ent.text,
                "label": ent.label_,
                "start": ent.start_char,
                "end": ent.end_char,
                "description": spacy.explain(ent.label_),
                "context": doc[max(0, ent.start-5):min(len(doc), ent.end+5)].text,
                "importance": self.classify_entity_importance(ent, doc)
            }
            entities.append(entity)
        return entities
    
    def classify_entity_importance(self, ent, doc) -> float:
        """Score entity importance with optimized calculations."""
        importance = 1.0
        
        if ent.start < len(doc) * 0.2:
            importance *= 1.2
        
        frequency = sum(1 for e in doc.ents if e.text.lower() == ent.text.lower())
        importance *= (1 + (frequency - 1) * 0.1)
        
        if ent.text.istitle():
            importance *= 1.1
        
        if len(ent.text.split()) > 1:
            importance *= 1.1
            
        return importance
    
    def process_ner_batch(self, texts: List[str]) -> List[List[Dict]]:
        """Process NER in optimized batches using Hugging Face pipeline."""
        try:
            ner_results = self.ner_pipeline(texts, aggregation_strategy="simple")
            entities = []
            for res in ner_results:
                doc_entities = []
                for entity in res:
                    doc_entities.append({
                        'text': entity['word'],
                        'label': entity['entity_group'],
                        'start': entity['start'],
                        'end': entity['end']
                    })
                entities.append(doc_entities)
            return entities
        except Exception as e:
            self.logger.error(f"NER processing error: {e}")
            return [[] for _ in texts]
    
    def process_keywords_batch_parallel(self, texts: List[str], batch_size: int) -> List[List[Dict[str, float]]]:
        """Process keywords in optimized parallel batches using KeyBERT."""
        try:
            keywords = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(self.keyword_extractor.extract_keywords, text, keyphrase_ngram_range=(1, 2), stop_words='english'): idx for idx, text in enumerate(texts)}
                
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        kw = future.result()
                        keywords.append([
                            {'word': keyword, 'score': float(score)}
                            for keyword, score in kw
                        ])
                    except Exception as e:
                        self.logger.error(f"Keyword extraction error for text index {idx}: {e}")
                        keywords.append([])
            
            return keywords
                
        except Exception as e:
            self.logger.error(f"Parallel Keyword extraction error with KeyBERT: {e}")
            return [[] for _ in texts]

    
    def create_temp_directory(self) -> str:
        """Create and track a temporary directory."""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        self.logger.debug(f"Created temporary directory: {temp_dir}")
        return temp_dir
    
    def cleanup_temp_directories(self) -> None:
        """Clean up all temporary directories."""
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir)
                self.logger.debug(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up temp directory {temp_dir}: {e}")
        self.temp_dirs.clear()
    
    def cleanup(self) -> None:
        """Enhanced cleanup with GPU memory management."""
        try:
            self.embedding_model = None
            self.ner_pipeline = None
            self.keyword_extractor = None
            
            if self.use_gpu:
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
            
            self.cleanup_temp_directories()
            
            gc.collect()
            
            self.logger.info("ML models, temp directories, and GPU memory cleaned up")
            
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")
            raise

class WeaviateIndexer:
    """Handles Weaviate client connection, schema creation, and object insertion."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.client = None
        self.collection_name = "Email"
        
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
            if not self.client.collections.exists(self.collection_name):
                self.client.collections.create(
                    name=self.collection_name,
                    vectorizer_config=None,
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
                            indexRangeFilters=True,
                            description="Email timestamp in ISO format"
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
                                    data_type=DataType.INT,
                                    indexFilterable=True,
                                    indexRangeFilters=True
                                ),
                                Property(
                                    name="end",
                                    data_type=DataType.INT,
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
                                )
                            ]
                        )
                    ]
                )
                self.logger.info("Email schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating Email schema: {e}")
            raise

    @retry(max_attempts=3, delay=1, backoff=2)
    def insert_objects_batch(self, email_objs: List[Dict[str, Any]]) -> Dict[str, List]:
        """Insert objects with enhanced error tracking."""
        if not email_objs:
            return {'failed_objects': []}
            
        try:
            collection = self.client.collections.get(self.collection_name)
            failed_objects = []
            worker_id = multiprocessing.current_process().name
            
            self.logger.info(f"[{worker_id}] Attempting to insert batch of {len(email_objs)} objects")
            
            with collection.batch.dynamic() as batch:
                for i, email_obj in enumerate(email_objs):
                    try:
                        prepared_obj = self._prepare_email_object(email_obj)
                        vector = email_obj.get('body_vector', [0.0] * 384)
                        batch.add_object(
                            properties=prepared_obj,
                            vector=vector
                        )
                    except Exception as e:
                        failed_objects.append(email_obj)
                        self.logger.error(f"[{worker_id}] Failed to add object {i} to batch: {e}")

            successful_count = len(email_objs) - len(failed_objects)
            self.logger.info(
                f"[{worker_id}] Batch insertion complete: "
                f"{successful_count}/{len(email_objs)} successful, "
                f"{len(failed_objects)} failed"
            )
                        
            return {'failed_objects': failed_objects}
            
        except Exception as e:
            self.logger.error(f"[{worker_id}] Complete batch insertion failed: {e}")
            return {'failed_objects': email_objs}

    def _prepare_email_object(self, email_obj: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare email object for insertion."""
        obj = email_obj.copy()
        
        obj.pop('body_vector', None)
        
        if 'date_val' in obj:
            try:
                timestamp = int(obj['date_val'])
                obj['date_val'] = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            except Exception as e:
                self.logger.error(f"Failed to convert date: {e}")
                obj['date_val'] = datetime.now(timezone.utc).isoformat()
        
        return obj

    def close_connection(self) -> None:
        """Safely close Weaviate connection."""
        try:
            if self.client:
                self.client.close()
                self.logger.info("Weaviate client connection closed")
        except Exception as e:
            self.logger.error(f"Error closing Weaviate connection: {e}")
            raise

class EmailProcessor:
    """Enhanced EmailProcessor with detailed logging."""
    
    def __init__(self, logger: logging.Logger, ml_models: MLModels, weaviate_indexer: WeaviateIndexer):
        self.logger = logger
        self.ml_models = ml_models
        self.weaviate_indexer = weaviate_indexer
        
        self.checkpoint_file = Path("email_indexing_checkpoint.json")
        self.processed_count = Value('i', self._load_checkpoint())
        
        self.parsed_cache: Dict[str, Any] = {}
        self.parse_time = 0
        self.enrich_time = 0
        self.index_time = 0

        self.start_time = time.time()
        self.stats = {
            'total_messages': Value('i', 0),
            'total_processed': Value('i', 0),
            
            'parse_attempts': Value('i', 0),
            'parse_successes': Value('i', 0),
            'parse_failures': Value('i', 0),
            'parse_empty_body': Value('i', 0),
            'parse_encoding_errors': Value('i', 0),
            'parse_missing_fields': Value('i', 0),
            
            'enrichment_attempts': Value('i', 0),
            'enrichment_successes': Value('i', 0),
            'enrichment_failures': Value('i', 0),
            'embedding_errors': Value('i', 0),
            'ner_errors': Value('i', 0),
            'keyword_errors': Value('i', 0),
            
            'insertion_attempts': Value('i', 0),
            'insertion_successes': Value('i', 0),
            'insertion_failures': Value('i', 0),
            'batch_failures': Value('i', 0),
            
            'parse_time': Value('d', 0.0),
            'enrichment_time': Value('d', 0.0),
            'insertion_time': Value('d', 0.0),
            
            'parse_empty_messages': Value('i', 0),
            'parse_invalid_format': Value('i', 0),
            
            'enrichment_empty_texts': Value('i', 0),
            'enrichment_too_long': Value('i', 0),
            
            'batch_objects_attempted': Value('i', 0),
            'batch_objects_failed': Value('i', 0),
            'weaviate_validation_errors': Value('i', 0),
        }

        self.PARSING_WORKERS = min(8, os.cpu_count() or 1)

    def batch_parse_raw_messages(self, raw_msgs: List[str]) -> Tuple[List[Optional[EmailMetadata]], List[str]]:
        """
        Parse raw messages using ThreadPoolExecutor for better CPU utilization.
        
        :param raw_msgs: List of raw email messages
        :return: Tuple of (metadata list, body list)
        """
        meta_list: List[Optional[EmailMetadata]] = []
        body_list: List[str] = []
        worker_id = multiprocessing.current_process().name

        parsing_stats = {
            'total': len(raw_msgs),
            'success': 0,
            'empty_body': 0,
            'parse_error': 0,
            'invalid_encoding': 0,
            'missing_fields_recovered': 0
        }

        with ThreadPoolExecutor(max_workers=self.PARSING_WORKERS) as executor:
            future_to_msg = {executor.submit(self._parse_single_message, raw): raw for raw in raw_msgs}
            
            for future in as_completed(future_to_msg):
                raw = future_to_msg[future]
                try:
                    meta, body = future.result()
                    
                    if meta is None:
                        parsing_stats['parse_error'] += 1
                        meta_list.append(None)
                        body_list.append("")
                        continue

                    if not meta.message_id or not meta.subject:
                        parsing_stats['missing_fields_recovered'] += 1
                        if not meta.message_id:
                            meta.message_id = str(uuid.uuid4())
                        if not meta.subject:
                            meta.subject = "(No Subject)"

                    parsing_stats['success'] += 1
                    meta_list.append(meta)
                    body_list.append(body)

                except Exception as e:
                    self.logger.error(f"[{worker_id}] Error parsing message: {e}")
                    parsing_stats['parse_error'] += 1
                    meta_list.append(None)
                    body_list.append("")

        parse_end_time = time.time()
        parse_duration = parse_end_time - self.start_time
        with self.stats['parse_time'].get_lock():
            self.stats['parse_time'].value += parse_duration

        with self.stats['parse_successes'].get_lock():
            self.stats['parse_successes'].value += parsing_stats['success']
        with self.stats['parse_failures'].get_lock():
            self.stats['parse_failures'].value += parsing_stats['parse_error']
        with self.stats['parse_empty_body'].get_lock():
            self.stats['parse_empty_body'].value += parsing_stats['empty_body']
        with self.stats['parse_encoding_errors'].get_lock():
            self.stats['parse_encoding_errors'].value += parsing_stats['invalid_encoding']
        with self.stats['parse_missing_fields'].get_lock():
            self.stats['parse_missing_fields'].value += parsing_stats['missing_fields_recovered']

        success_rate = (parsing_stats['success'] / parsing_stats['total']) * 100 if parsing_stats['total'] > 0 else 0
        self.logger.info(
            f"[{worker_id}] Parsing results:\n"
            f"  Total messages: {parsing_stats['total']}\n"
            f"  Successfully parsed: {parsing_stats['success']} ({success_rate:.1f}%)\n"
            f"  Empty bodies: {parsing_stats['empty_body']}\n"
            f"  Encoding errors: {parsing_stats['invalid_encoding']}\n"
            f"  Parse errors: {parsing_stats['parse_error']}\n"
            f"  Missing fields recovered: {parsing_stats['missing_fields_recovered']}"
        )
        
        return meta_list, body_list

    def generate_summary(self) -> str:
        """Generate a detailed processing summary using existing stats dictionary."""
        elapsed_time = time.time() - self.start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)
        
        def pct(part: Value, whole: Value) -> float:
            return (part.value / whole.value * 100) if whole.value > 0 else 0
        
        def rate(count: Value, duration: Value) -> float:
            return count.value / duration.value if duration.value > 0 else 0
        
        try:
            weaviate_count = self.weaviate_indexer.client.query.aggregate(
                self.weaviate_indexer.collection_name
            ).with_meta_count().do()
            weaviate_objects = weaviate_count['data']['Aggregate']['Meta']['count']
        except Exception as e:
            self.logger.error(f"Failed to get Weaviate object count: {e}")
            weaviate_objects = "Unknown"

        summary = [
            "📊 Email Processing Pipeline Summary",
            "=" * 50,
            f"\n⏱️ Timing",
            f"Total Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}",
            f"Parse Time: {self.stats['parse_time']:.1f}s ({rate(self.stats['parse_successes'], self.stats['parse_time']):.1f} msgs/sec)",
            f"Enrichment Time: {self.stats['enrichment_time']:.1f}s ({rate(self.stats['enrichment_successes'], self.stats['enrichment_time']):.1f} msgs/sec)",
            f"Insertion Time: {self.stats['insertion_time']:.1f}s ({rate(self.stats['insertion_successes'], self.stats['insertion_time']):.1f} msgs/sec)",
            
            f"\n📥 Input Statistics",
            f"Total Messages: {self.stats['total_messages']:,}",
            f"Total Processed: {self.stats['total_processed']:,}",
            
            f"\n🔍 Parsing Stage",
            f"Attempts: {self.stats['parse_attempts']:,}",
            f"Successes: {self.stats['parse_successes']:,} ({pct(self.stats['parse_successes'], self.stats['parse_attempts']):.1f}%)",
            f"Failures: {self.stats['parse_failures']:,} ({pct(self.stats['parse_failures'], self.stats['parse_attempts']):.1f}%)",
            f"Empty Bodies: {self.stats['parse_empty_body']:,}",
            f"Encoding Errors: {self.stats['parse_encoding_errors']:,}",
            f"Missing Fields: {self.stats['parse_missing_fields']:,}",
            
            f"\n🔮 Enrichment Stage",
            f"Attempts: {self.stats['enrichment_attempts']:,}",
            f"Successes: {self.stats['enrichment_successes']:,} ({pct(self.stats['enrichment_successes'], self.stats['enrichment_attempts']):.1f}%)",
            f"Failures: {self.stats['enrichment_failures']:,} ({pct(self.stats['enrichment_failures'], self.stats['enrichment_attempts']):.1f}%)",
            f"Embedding Errors: {self.stats['embedding_errors']:,}",
            f"NER Errors: {self.stats['ner_errors']:,}",
            f"Keyword Errors: {self.stats['keyword_errors']:,}",
            
            f"\n💾 Database Insertion Stage",
            f"Attempts: {self.stats['insertion_attempts']:,}",
            f"Successes: {self.stats['insertion_successes']:,} ({pct(self.stats['insertion_successes'], self.stats['insertion_attempts']):.1f}%)",
            f"Failures: {self.stats['insertion_failures']:,} ({pct(self.stats['insertion_failures'], self.stats['insertion_attempts']):.1f}%)",
            f"Batch Failures: {self.stats['batch_failures']:,}",
            
            f"\n📈 Pipeline Efficiency",
            f"Overall Success Rate: {pct(self.stats['insertion_successes'], self.stats['total_messages']):.1f}%",
            f"Average Processing Rate: {rate(self.stats['insertion_successes'], elapsed_time):.1f} msgs/sec",
            f"Message Loss: {self.stats['total_messages'] - self.stats['insertion_successes']:,} messages",

            f"\n🔍 Detailed Parsing Losses",
            f"Empty Messages: {self.stats['parse_empty_messages']:,}",
            f"Invalid Format: {self.stats['parse_invalid_format']:,}",
            f"Empty Bodies: {self.stats['parse_empty_body']:,}",
            f"Encoding Errors: {self.stats['parse_encoding_errors']:,}",
            f"Missing Fields: {self.stats['parse_missing_fields']:,}",
            
            f"\n🔮 Detailed Enrichment Losses",
            f"Empty Texts Filtered: {self.stats['enrichment_empty_texts']:,}",
            f"Texts Truncated: {self.stats['enrichment_too_long']:,}",
            f"Embedding Errors: {self.stats['embedding_errors']:,}",
            f"NER Errors: {self.stats['ner_errors']:,}",
            f"Keyword Errors: {self.stats['keyword_errors']:,}",
            
            f"\n💾 Detailed Insertion Losses",
            f"Batch Objects Attempted: {self.stats['batch_objects_attempted']:,}",
            f"Batch Objects Failed: {self.stats['batch_objects_failed']:,}",
            f"Validation Errors: {self.stats['weaviate_validation_errors']:,}",
            
            f"\n📊 Final Pipeline Summary",
            f"Input Messages: {self.stats['total_messages']:,}",
            f"Successfully Processed: {self.stats['total_processed']:,}",
            f"Weaviate Objects: {weaviate_objects:,}",
            f"Total Pipeline Loss: {self.stats['total_messages'] - weaviate_objects:,} messages",
            
            f"\n💡 Loss Breakdown",
            f"Parsing Stage Loss: {self.stats['total_messages'] - self.stats['parse_successes']:,} messages",
            f"Enrichment Stage Loss: {self.stats['parse_successes'] - self.stats['enrichment_successes']:,} messages",
            f"Insertion Stage Loss: {self.stats['enrichment_successes'] - self.stats['insertion_successes']:,} messages",
            
            "=" * 50
        ]
        
        return "\n".join(summary)

    def process_chunk(self, raw_msgs: List[str]) -> None:
        chunk_start = time.time()
        worker_id = multiprocessing.current_process().name
        
        try:
            with self.stats['total_messages'].get_lock():
                self.stats['total_messages'].value += len(raw_msgs)
            
            parse_start = time.time()
            meta_list, body_list = self.batch_parse_raw_messages(raw_msgs)
            parse_time = time.time() - parse_start
            
            with self.stats['parse_time'].get_lock():
                self.stats['parse_time'].value += parse_time
            
            valid_msgs = sum(1 for m in meta_list if m is not None)
            
            with self.stats['parse_successes'].get_lock():
                self.stats['parse_successes'].value += valid_msgs
            with self.stats['parse_failures'].get_lock():
                self.stats['parse_failures'].value += len(raw_msgs) - valid_msgs
            
            enrich_start = time.time()
            enriched = self.batch_enrich(meta_list, body_list)
            enrich_time = time.time() - enrich_start
            
            with self.stats['enrichment_time'].get_lock():
                self.stats['enrichment_time'].value += enrich_time
            
            if enriched:
                index_start = time.time()
                with self.stats['insertion_attempts'].get_lock():
                    self.stats['insertion_attempts'].value += len(enriched)
                
                result = self.weaviate_indexer.insert_objects_batch(enriched)
                index_time = time.time() - index_start
                
                with self.stats['insertion_time'].get_lock():
                    self.stats['insertion_time'].value += index_time
                
                failed_objects = result.get('failed_objects', [])
                successful_insertions = len(enriched) - len(failed_objects)
                
                with self.stats['insertion_successes'].get_lock():
                    self.stats['insertion_successes'].value += successful_insertions
                with self.stats['insertion_failures'].get_lock():
                    self.stats['insertion_failures'].value += len(failed_objects)
                with self.stats['batch_objects_attempted'].get_lock():
                    self.stats['batch_objects_attempted'].value += len(enriched)
                with self.stats['batch_objects_failed'].get_lock():
                    self.stats['batch_objects_failed'].value += len(failed_objects)
                
        except Exception as e:
            self.logger.error(f"[{worker_id}] Chunk processing error: {e}")
            traceback.print_exc()
            with self.stats['parse_failures'].get_lock():
                self.stats['parse_failures'].value += len(raw_msgs)

    def batch_enrich(self, meta_list: List[Optional[EmailMetadata]], body_list: List[str]) -> List[Dict[str, Any]]:
        """Batch enrichment with detailed progress logging and error tracking."""
        worker_id = multiprocessing.current_process().name
        
        try:
            initial_count = len(meta_list)
            with self.stats['enrichment_attempts'].get_lock():
                self.stats['enrichment_attempts'].value += initial_count
            
            texts = []
            valid_indices = []
            for i, (meta, body) in enumerate(zip(meta_list, body_list)):
                if not meta or not body:
                    with self.stats['enrichment_empty_texts'].get_lock():
                        self.stats['enrichment_empty_texts'].value += 1
                    continue
                    
                text = body[:self.ml_models.embedding_model.max_seq_length].strip()
                if not text:
                    with self.stats['enrichment_empty_texts'].get_lock():
                        self.stats['enrichment_empty_texts'].value += 1
                    continue
                
                if len(text) > self.ml_models.embedding_model.max_seq_length:
                    with self.stats['enrichment_too_long'].get_lock():
                        self.stats['enrichment_too_long'].value += 1
                        
                texts.append(text)
                valid_indices.append(i)

            if not texts:
                self.logger.info(f"[{worker_id}] No valid texts to enrich")
                return []

            self.logger.info(f"[{worker_id}] Starting enrichment of {len(texts)} texts")
            
            try:
                embed_start = time.time()
                embeddings, base_entities, keywords = self.ml_models.process_texts_parallel(
                    texts,
                    batch_size=EMBEDDING_BATCH_SIZE
                )
                embed_time = time.time() - embed_start
                
                self.logger.info(
                    f"[{worker_id}] Generated embeddings and entities "
                    f"in {embed_time:.2f}s "
                    f"({len(texts)/embed_time:.1f} texts/sec)"
                )
                
            except Exception as process_err:
                self.logger.error(f"[{worker_id}] Text processing error: {process_err}")
                with self.stats['enrichment_failures'].get_lock():
                    self.stats['enrichment_failures'].value += len(texts)
                with self.stats['embedding_errors'].get_lock():
                    self.stats['embedding_errors'].value += len(texts)
                
                fallback_emails = []
                for idx, (meta, body) in enumerate(zip(meta_list, body_list)):
                    if idx in valid_indices and meta:
                        fallback_dict = {
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
                            "keywords": []
                        }
                        fallback_emails.append(fallback_dict)
                        with self.stats['enrichment_successes'].get_lock():
                            self.stats['enrichment_successes'].value += 1
                return fallback_emails
            
            enriched_emails = []
            valid_result_idx = 0
            
            for idx, (meta, body) in enumerate(zip(meta_list, body_list)):
                if idx not in valid_indices:
                    continue
                    
                try:
                    doc = self.ml_models.nlp(body[:MAX_BODY_LENGTH])
                    try:
                        enhanced_entities = self.ml_models.process_entities(doc)
                    except Exception as ner_err:
                        self.logger.error(f"[{worker_id}] NER processing error for document {idx}: {ner_err}")
                        with self.stats['ner_errors'].get_lock():
                            self.stats['ner_errors'].value += 1
                        enhanced_entities = []

                    try:
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
                            "keywords": keywords[valid_result_idx] if valid_result_idx < len(keywords) else []
                        }
                        
                        valid_result_idx += 1
                        enriched_emails.append(email_dict)
                        with self.stats['enrichment_successes'].get_lock():
                            self.stats['enrichment_successes'].value += 1
                        
                    except Exception as e:
                        self.logger.error(f"[{worker_id}] Error creating email dict for index {idx}: {e}")
                        fallback_dict = {
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
                            "keywords": []
                        }
                        enriched_emails.append(fallback_dict)
                        with self.stats['enrichment_failures'].get_lock():
                            self.stats['enrichment_failures'].value += 1
                        with self.stats['enrichment_successes'].get_lock():
                            self.stats['enrichment_successes'].value += 1
                    
                except Exception as e:
                    self.logger.error(f"[{worker_id}] Error enriching email {idx}: {e}")
                    if meta:
                        fallback_dict = {
                            "message_id": meta.message_id,
                            "subject": meta.subject,
                            "sender": meta.sender,
                            "recipients": meta.recipients,
                            "date_val": meta.date_val,
                            "thread_id": meta.thread_id,
                            "labels": meta.labels,
                            "body": body[:MAX_BODY_LENGTH] if body else "",
                            "body_vector": [0.0] * 384,
                            "entities": [],
                            "keywords": []
                        }
                        enriched_emails.append(fallback_dict)
                        with self.stats['enrichment_failures'].get_lock():
                            self.stats['enrichment_failures'].value += 1
                        with self.stats['enrichment_successes'].get_lock():
                            self.stats['enrichment_successes'].value += 1
            
            self.logger.info(f"[{worker_id}] Completed enrichment of {len(enriched_emails)} emails")
            
            success_rate = (len(enriched_emails) / initial_count * 100) if initial_count > 0 else 0
            self.logger.info(
                f"[{worker_id}] Enrichment success rate: {success_rate:.1f}% "
                f"({len(enriched_emails)}/{initial_count})"
            )
            
            return enriched_emails

        except Exception as e:
            self.logger.error(f"[{worker_id}] Comprehensive batch enrichment error: {e}")
            with self.stats['enrichment_failures'].get_lock():
                self.stats['enrichment_failures'].value += len(meta_list)
                
            fallback_emails = []
            for meta, body in zip(meta_list, body_list):
                if meta:
                    fallback_dict = {
                        "message_id": meta.message_id,
                        "subject": meta.subject,
                        "sender": meta.sender,
                        "recipients": meta.recipients,
                        "date_val": meta.date_val,
                        "thread_id": meta.thread_id,
                        "labels": meta.labels,
                        "body": body[:MAX_BODY_LENGTH] if body else "",
                        "body_vector": [0.0] * 384,
                        "entities": [],
                        "keywords": []
                    }
                    fallback_emails.append(fallback_dict)
                    with self.stats['enrichment_successes'].get_lock():
                        self.stats['enrichment_successes'].value += 1
            return fallback_emails

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
        """Save processing checkpoint with synchronized count."""
        try:
            temp_checkpoint = self.checkpoint_file.with_suffix('.tmp')
            temp_checkpoint.write_bytes(
                orjson.dumps({
                    "last_processed_index": self.processed_count.value,
                    "timestamp": datetime.now().isoformat()
                })
            )
            temp_checkpoint.replace(self.checkpoint_file)
            
            self.logger.debug(f"Checkpoint saved: {self.processed_count.value}")
        except Exception as e:
            self.logger.error(f"Checkpoint save failed: {e}")

    def _parse_email_cached(self, raw_msg: str) -> Tuple[Optional[EmailMetadata], str]:
        """
        Cached email parsing with robust encoding handling.
        
        :param raw_msg: Raw email message string
        :return: Tuple of (metadata, body)
        """
        try:
            msg_hash = hashlib.md5(raw_msg.encode('utf-8', errors='ignore')).hexdigest()
            
            if msg_hash in self.parsed_cache:
                return self.parsed_cache[msg_hash]
            
            try:
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
        Decode bytes using ftfy for robust and efficient text correction.
        
        :param raw_msg: Raw email message bytes
        :return: Decoded email message string
        """
        try:
            decoded = ftfy.fix_text(raw_msg.decode('utf-8', errors='replace'))
            return decoded
        except Exception as e:
            self.logger.error(f"ftfy decoding failed: {e}")
            return raw_msg.decode('latin1', errors='replace')

    def _parse_single_message(self, raw_msg: str) -> Tuple[Optional[EmailMetadata], str]:
            """
            Parse a single message and return metadata and body.
            
            :param raw_msg: Raw email message string
            :return: Tuple of (EmailMetadata or None, email body string)
            """
            try:
                if not raw_msg:
                    return None, ""
                
                if isinstance(raw_msg, bytes):
                    raw_msg = self._decode_bytes(raw_msg)
                
                meta, body = self._parse_email_cached(raw_msg)
                
                if meta is None:
                    return None, ""
                
                if not meta.message_id:
                    meta.message_id = str(uuid.uuid4())
                if not meta.subject:
                    meta.subject = "(No Subject)"
                
                return meta, body
            except Exception as e:
                self.logger.error(f"Error parsing single message: {e}")
                return None, ""

    @retry(max_attempts=3, delay=1, backoff=2)
    def parse_email_single(self, raw_msg: str) -> Optional[email.message.EmailMessage]:
        """
        Parse a single raw email message with robust error handling.
        
        :param raw_msg: Raw email message string
        :return: Parsed email message object or None
        """
        try:
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
        Extract structured metadata with robust date/thread handling and encoding.
        
        Args:
            msg: Parsed email message
        Returns:
            EmailMetadata object or None if extraction fails
        """
        try:
            def safe_decode(value: Union[str, bytes, None], fallback: str = "") -> str:
                if value is None:
                    return fallback
                
                if isinstance(value, bytes):
                    try:
                        decoded = value.decode('utf-8', errors='replace')
                    except Exception:
                        try:
                            decoded = value.decode('latin-1', errors='replace')
                        except Exception:
                            decoded = str(value)
                    return decoded
                return str(value)

            date_str = msg.get("Date")
            if date_str:
                try:
                    date_tuple = email.utils.parsedate_tz(date_str)
                    if date_tuple:
                        timestamp = email.utils.mktime_tz(date_tuple)
                    else:
                        timestamp = int(time.time())
                except Exception as e:
                    self.logger.warning(f"Date parsing failed: {e}, using current time")
                    timestamp = int(time.time())
            else:
                timestamp = int(time.time())

            thread_id = msg.get("Thread-ID")
            if not thread_id:
                subject = safe_decode(msg.get("Subject", ""))
                msg_id = safe_decode(msg.get("Message-ID", ""))
                references = safe_decode(msg.get("References", ""))
                
                thread_data = f"{subject}:{msg_id}:{references}"
                thread_id = hashlib.sha256(thread_data.encode()).hexdigest()[:16]
            
            message_id = safe_decode(msg.get("Message-ID"))
            if not message_id:
                message_id = str(uuid.uuid4())

            raw_labels = msg.get("X-Gmail-Labels", "").split(",")
            labels = [
                safe_decode(label.strip()) 
                for label in raw_labels 
                if label.strip()
            ]

            return EmailMetadata(
                message_id=message_id,
                subject=safe_decode(msg.get("Subject")),
                sender=safe_decode(msg.get("From")),
                recipients=safe_decode(msg.get("To")),
                date_val=timestamp,
                thread_id=thread_id,
                labels=labels
            )
        except Exception as e:
            self.logger.error(f"Metadata extraction failed: {e}")
            return None

    def extract_email_body(self, msg: email.message.EmailMessage) -> str:
        try:
            def safe_decode_payload(payload: bytes, charset: Optional[str] = None) -> str:
                """Safely decode email payload with multiple encoding strategies."""
                if not payload:
                    return ""

                encodings = [
                    charset,
                    'utf-8',
                    'latin-1',
                    'iso-8859-1',
                    'cp1252'
                ]

                encodings = [enc for enc in encodings if enc]

                for encoding in encodings:
                    try:
                        decoded = payload.decode(encoding, errors='replace')
                        return ''.join(
                            char for char in decoded
                            if char.isprintable() or char.isspace()
                        )
                    except Exception:
                        continue

                # Last resort
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

                if not body_parts:
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        if content_type == "text/html" and not part.get("Content-Disposition"):
                            payload = part.get_payload(decode=True)
                            charset = part.get_content_charset()

                            if payload:
                                decoded_html = safe_decode_payload(payload, charset)
                                try:
                                    # Attempt parsing with 'lxml' first
                                    try:
                                        soup = BeautifulSoup(decoded_html, 'lxml')
                                    except Exception as e_lxml:
                                        self.logger.error(f"lxml parsing failed: {e_lxml}")
                                        # Fallback to 'html5lib'
                                        try:
                                            soup = BeautifulSoup(decoded_html, 'html5lib')
                                        except Exception as e_html5lib:
                                            self.logger.error(f"html5lib parsing failed: {e_html5lib}")
                                            # Last resort to 'html.parser'
                                            soup = BeautifulSoup(decoded_html, 'html.parser')

                                    text = soup.get_text(separator='\n', strip=True)
                                    body_parts.append(text)
                                except Exception as e:
                                    self.logger.error(f"BeautifulSoup parsing error: {e}")
                                    self.logger.error(traceback.format_exc())
                                break  # Assuming only one text/html part is needed
                body = "\n".join(body_parts)[:MAX_BODY_LENGTH]
            else:
                payload = msg.get_payload(decode=True)
                charset = msg.get_content_charset()

                body = safe_decode_payload(payload, charset)[:MAX_BODY_LENGTH]

                if msg.get_content_type() == "text/html":
                    try:
                        # Attempt parsing with 'lxml' first
                        try:
                            soup = BeautifulSoup(body, 'lxml')
                        except Exception as e_lxml:
                            self.logger.error(f"lxml parsing failed: {e_lxml}")
                            # Fallback to 'html5lib'
                            try:
                                soup = BeautifulSoup(body, 'html5lib')
                            except Exception as e_html5lib:
                                self.logger.error(f"html5lib parsing failed: {e_html5lib}")
                                # Last resort to 'html.parser'
                                soup = BeautifulSoup(body, 'html.parser')

                        body = soup.get_text(separator='\n', strip=True)
                    except Exception as e:
                        self.logger.error(f"BeautifulSoup parsing error: {e}")
                        self.logger.error(traceback.format_exc())

            return body.strip()

        except Exception as e:
            self.logger.error(f"Body extraction error: {e}")
            self.logger.error(traceback.format_exc())
            return ""
        def cleanup(self):
            """Print final statistics during cleanup."""
            self.logger.info("\n" + self.generate_summary())

class ProcessPool:
    """
    Manages a pool of worker processes with advanced coordination 
    and graceful shutdown mechanisms.
    """
    
    def __init__(self, num_workers: int, mbox_path: str, gpu_ids: List[int]):
        self.num_workers = num_workers
        self.mbox_path = mbox_path
        self.workers: List[Process] = []
        self.stop_event = Event()
        self.processed_count = Value(ctypes.c_int, 0)
        self.error_count = Value(ctypes.c_int, 0)
        self.queue = MPQueue(maxsize=num_workers * 4)
        self.gpu_ids = gpu_ids

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
            self.stop_event.set()
            
            for _ in range(self.num_workers):
                try:
                    self.queue.put(STOP_MARKER, block=False)
                except Exception:
                    pass
            
            for worker in self.workers:
                worker.join(timeout=10)
                
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
            
            self.queue.close()
            self.queue.join_thread()
            
            signal.signal(signal.SIGINT, self._original_sigint_handler)
            signal.signal(signal.SIGTERM, self._original_sigterm_handler)
            
        except Exception as e:
            print(f"Error during shutdown: {e}")
    
    def shutdown(self):
        """
        Comprehensive shutdown method.
        """
        try:
            self.stop()
        except Exception as e:
            print(f"Error during shutdown: {e}")
    
    def start_workers(self) -> None:
        """
        Start worker processes for email processing, assigning each a unique GPU.
        """
        for i in range(self.num_workers):
            gpu_id = self.gpu_ids[i % len(self.gpu_ids)]
            p = Process(
                target=self._worker_process_init,
                args=(
                    i, 
                    self.mbox_path, 
                    self.queue, 
                    self.stop_event, 
                    self.processed_count, 
                    self.error_count,
                    gpu_id
                )
            )
            p.daemon = False
            p.start()
            self.workers.append(p)
            print(f"Started worker {i} on GPU {gpu_id}")
   
    @staticmethod
    def _worker_process_init(worker_id: int, mbox_path: str, 
                            queue: MPQueue, stop_event: Event,
                            processed_count: Value, error_count: Value,
                            gpu_id: int) -> None:
        """Static method to initialize and run a worker process."""
        
        torch.cuda.set_device(gpu_id)
        
        logger = logging.getLogger(f"Worker-{worker_id}-GPU-{gpu_id}")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
        
        memory_manager = MemoryManager(logger)
        ml_models = None
        weaviate_indexer = None
        email_processor = None
        
        try:
            ml_models = MLModels(logger, gpu_id)
            ml_models.initialize()
            
            weaviate_indexer = WeaviateIndexer(logger)
            weaviate_indexer.connect()
            
            email_processor = EmailProcessor(logger, ml_models, weaviate_indexer)
            memory_monitor = MemoryMonitor(logger)

            logger.info(f"Worker {worker_id} initialized on GPU {gpu_id}")
            
            backoff = 1
            while not stop_event.is_set():
                try:
                    chunk = queue.get(timeout=backoff)
                    backoff = 1
                    
                    if chunk is STOP_MARKER:
                        logger.info(f"Worker {worker_id} received STOP_MARKER")
                        break
                    
                    if not memory_monitor.check_chunk_size(len(chunk)):
                        logger.warning("Memory limit exceeded, skipping chunk")
                        continue
                    
                    email_processor.process_chunk(chunk)
                    
                    with processed_count.get_lock():
                        processed_count.value += len(chunk)
                    
                    memory_manager.maybe_collect()
                        
                except Empty:
                    backoff = min(backoff * 2, 30)
                    continue
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}")
                    with error_count.get_lock():
                        error_count.value += 1
                    backoff = min(backoff * 2, 30)
                        
        except Exception as e:
            logger.error(f"Worker {worker_id} initialization failed: {e}")
            traceback.print_exc()
        finally:
            try:
                if email_processor:
                    del email_processor
                if ml_models:
                    ml_models.cleanup()
                    del ml_models  
                if weaviate_indexer:
                    weaviate_indexer.close_connection()
                    del weaviate_indexer
                
                memory_manager.maybe_collect()
                logger.info(f"Worker {worker_id} cleanup completed")
            except Exception as e:
                logger.error(f"Error during worker {worker_id} cleanup: {e}")

class UTF8Mbox(mailbox.mbox):
    """Custom mbox class that handles UTF-8 From lines."""
    
    def _generate_toc(self):
        """Generate table of contents with proper dictionary structure."""
        self._toc = {}
        self._file.seek(0)
        
        key = 0
        while True:
            line_pos = self._file.tell()
            line = self._file.readline()
            if line == b'': break
            if line.startswith(b'From '):
                self._toc[key] = line_pos
                key += 1
                
        self._next_key = len(self._toc)
        self._file.seek(0)
    
    def _parse_message(self, text):
        """Override to handle message parsing with policy."""
        return email.message_from_bytes(text, policy=email.policy.HTTP)
    
    def get_message(self, key):
        """Override to handle UTF-8 From lines and message boundaries."""
        start = self._toc[key]
        
        if key + 1 in self._toc:
            end = self._toc[key + 1]
        else:
            self._file.seek(0, 2)
            end = self._file.tell()
        
        self._file.seek(start)
        from_line = self._file.readline()
        message_text = self._file.read(end - self._file.tell())
        
        if isinstance(message_text, bytes):
            try:
                message_text = message_text.decode('utf-8', errors='replace')
            except UnicodeError:
                message_text = message_text.decode('latin1')
        
        return self._parse_message(message_text.encode('utf-8'))

def reader_process_init(mbox_path: str, queue: MPQueue, stop_event: Event) -> None:
    """
    Standalone reader process with custom UTF-8 mbox handling.
    """
    reader_logger = logging.getLogger("Reader")
    reader_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    reader_logger.addHandler(handler)
    
    start_time = time.time()
    msg_count = 0
    last_log_time = start_time
    error_count = 0
    
    def safe_encode_decode(message):
        """Safely encode/decode message content."""
        try:
            if isinstance(message, (str, bytes)):
                return str(message)
            
            for policy in [email.policy.HTTP, email.policy.SMTP]:
                try:
                    return message.as_string(policy=policy)
                except:
                    continue
                    
            try:
                raw = message.as_bytes()
                return raw.decode('utf-8', errors='replace')
            except:
                pass
                
            return str(message)
                
        except Exception as e:
            reader_logger.warning(f"Message conversion failed: {e}")
            return None

    try:
        file_size = os.path.getsize(mbox_path)
        reader_logger.info(
            f"Reader process started, opening {mbox_path} "
            f"(Size: {file_size / (1024*1024):.2f} MB)"
        )
        
        mbox = UTF8Mbox(mbox_path)
        batch_raw: List[str] = []
        
        reader_logger.info("Starting to process messages...")
        
        for msg in mbox:
            if stop_event.is_set():
                reader_logger.info("Stop event detected, terminating reader process")
                break

            try:
                raw_msg = safe_encode_decode(msg)
                if raw_msg:
                    batch_raw.append(raw_msg)
                    msg_count += 1

                    if len(batch_raw) >= BATCH_SIZE:
                        while not stop_event.is_set() and queue.qsize() >= QUEUE_MAX_SIZE:
                            time.sleep(0.1)
                        
                        if not stop_event.is_set():
                            queue.put(batch_raw)
                            
                            current_time = time.time()
                            if msg_count % 5000 == 0 or (current_time - last_log_time) >= 60:
                                elapsed = current_time - start_time
                                rate = msg_count / elapsed if elapsed > 0 else 0
                                reader_logger.info(
                                    f"Progress: {msg_count} messages processed "
                                    f"({rate:.1f} msgs/sec), "
                                    f"Queue size: {queue.qsize()}/{QUEUE_MAX_SIZE}, "
                                    f"Errors: {error_count}"
                                )
                                last_log_time = current_time
                        
                        batch_raw = []

            except Exception as e:
                error_count += 1
                if error_count % 1000 == 1:
                    reader_logger.error(f"Failed to process message: {e}")
                continue

        if batch_raw and not stop_event.is_set():
            try:
                queue.put(batch_raw)
                reader_logger.info(
                    f"Final batch queued: {len(batch_raw)} messages. "
                    f"Total processed: {msg_count}"
                )
            except Exception as e:
                reader_logger.error(f"Error queuing final batch: {e}")

    except Exception as e:
        reader_logger.error(f"Critical reader process error: {e}")
        traceback.print_exc()
    finally:
        end_time = time.time()
        elapsed = end_time - start_time
        rate = msg_count / elapsed if elapsed > 0 else 0
        
        reader_logger.info(
            f"Reader process finished. "
            f"Total messages: {msg_count}, "
            f"Time: {elapsed:.1f}s, "
            f"Rate: {rate:.1f} msgs/sec"
        )
        
        for _ in range(MAX_WORKERS):
            try:
                queue.put(STOP_MARKER)
            except Exception as e:
                reader_logger.error(f"Error placing stop marker: {e}")
        
        reader_logger.info("Stop markers placed for all workers")

class EmailIndexer:
    """
    Comprehensive email indexing system with advanced processing capabilities.
    """
    
    def __init__(self, mbox_path: str):
        configure_environment()
        configure_threading()
        
        self.mbox_path = Path(mbox_path)
        if not self.mbox_path.exists():
            raise FileNotFoundError(f"MBOX file not found at {self.mbox_path}")
        
        self.logger = self._setup_logging()
        
        self.gpu_ids = list(range(NUM_GPUS))
        
        self.process_pool = ProcessPool(MAX_WORKERS, str(self.mbox_path), self.gpu_ids)

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

        handlers = []
        
        file_handler = RotatingFileHandler(
            "indexing.log", 
            maxBytes=10*1024*1024,
            backupCount=5, 
            encoding="utf-8"
        )
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(name)s] %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        handlers.append(console_handler)

        for handler in handlers:
            logger.addHandler(handler)

        return logger
    
    def process_emails(self) -> None:
        """
        Coordinate email processing workflow.
        """
        try:
            self.logger.info("Starting email processing...")
            
            self.process_pool.start_workers()
            
            reader = Process(
                target=reader_process_init,
                args=(
                    str(self.mbox_path), 
                    self.process_pool.queue, 
                    self.process_pool.stop_event
                )
            )
            reader.start()
            
            reader.join()
            
            self.process_pool.shutdown()
            
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
            if hasattr(self, 'process_pool'):
                self.process_pool.stop()
            
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
    try:
       configure_interrupt_handling()
       load_dotenv()
       
       if len(sys.argv) > 1:
           mbox_path = os.path.abspath(sys.argv[1])
       else:
           mbox_path = os.path.abspath("../data/mbox_export.mbox")
       
       if not os.path.exists(mbox_path):
           raise FileNotFoundError(f"MBOX file not found at {mbox_path}")

       with EmailIndexer(mbox_path) as indexer:
           indexer.run()

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Performing cleanup...")
    except Exception as e:
        print(f"Fatal error during email indexing: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    finally:
        for p in multiprocessing.active_children():
            p.terminate()
            p.join(timeout=5)

if __name__ == "__main__":
    main()
