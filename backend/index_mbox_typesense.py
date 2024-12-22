"""
Enhanced Email Indexing System with improved performance, observability, and error handling.
"""

import os
import gc
import json
import time
import uuid
import logging
import psutil
import sys
import mailbox
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from email.header import decode_header
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Third-party imports
import typesense
import torch
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
from transformers import pipeline
import spacy
from rake_nltk import Rake
from gensim import corpora, models
import pandas as pd
from dotenv import load_dotenv

# Constants
MAX_BODY_LENGTH = 100_000
BATCH_SIZE = 10
LOG_INTERVAL = 1000
MAX_RETRIES = 5
MAX_WORKERS = 4

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
    summary: str

class MLModels:
    """Container for ML models to ensure proper initialization and cleanup."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.embedding_model: Optional[SentenceTransformer] = None
        self.summarizer: Optional[Any] = None
        self.nlp: Optional[Any] = None
        self.rake: Optional[Rake] = None

    def initialize(self) -> None:
        """Initialize all ML models with proper error handling."""
        try:
            self._setup_gpu_environment()
            self._initialize_embedding_model()
            self._initialize_nlp()
            self._initialize_summarizer()
            self._initialize_rake()
        except Exception as e:
            self.logger.error(f"Failed to initialize ML models: {e}")
            raise

    def _setup_gpu_environment(self) -> None:
        """Configure GPU environment settings."""
        os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    def _initialize_embedding_model(self) -> None:
        """Initialize the sentence transformer model that produces 1536-dimensional vectors."""
        try:
            gc.collect()
            self.embedding_model = SentenceTransformer(
                'Alibaba-NLP/gte-Qwen2-1.5B-instruct',
                device='cpu'
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
        except OSError:
            self.logger.warning("SpaCy model not found, downloading...")
            os.system("python -m spacy download en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")

    def _initialize_summarizer(self) -> None:
        """Initialize the summarization pipeline."""
        try:
            self.summarizer = pipeline(
                "summarization",
                model="pszemraj/long-t5-tglobal-base-16384-book-summary",
                device=-1,  # Force CPU
                batch_size=1
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize summarizer: {e}")
            raise

    def _initialize_rake(self) -> None:
        """Initialize RAKE keyword extractor."""
        self.rake = Rake()

    def cleanup(self) -> None:
        """Clean up ML models and free memory."""
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

class EmailProcessor:
    """Main email processing class with improved error handling and observability."""

    def __init__(self, logger: logging.Logger, ml_models: MLModels, typesense_client: typesense.Client):
        self.logger = logger
        self.ml_models = ml_models
        self.typesense_client = typesense_client
        self.checkpoint_file = Path("checkpoint.json")
        self.processed_count = self._load_checkpoint()

    def _load_checkpoint(self) -> int:
        """Load processing checkpoint with proper error handling."""
        try:
            if self.checkpoint_file.exists():
                data = json.loads(self.checkpoint_file.read_text())
                return data.get("last_processed_index", 0)
            return 0
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return 0

    def _save_checkpoint(self) -> None:
        """Save processing checkpoint with error handling."""
        try:
            self.checkpoint_file.write_text(
                json.dumps({"last_processed_index": self.processed_count})
            )
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    @staticmethod
    def decode_header_value(value: str) -> str:
        """Decode email header values with improved error handling."""
        if not value:
            return ""
        try:
            decoded_parts = decode_header(value)
            return " ".join(
                text.decode(charset or 'utf-8', errors='replace')
                if isinstance(text, bytes) else str(text)
                for text, charset in decoded_parts
            )
        except Exception:
            return value

    def extract_email_body(self, msg: mailbox.mboxMessage) -> str:
        """Extract email body with improved HTML handling and cleanup."""
        def process_part(part: Any) -> str:
            if part.get_content_disposition() == 'attachment':
                return ""
            
            payload = part.get_payload(decode=True)
            if not payload:
                return ""
                
            try:
                decoded = payload.decode(errors="replace")
                return (strip_html(decoded) 
                       if part.get_content_type() == 'text/html'
                       else decoded)
            except Exception as e:
                self.logger.error(f"Error decoding part: {e}")
                return ""

        try:
            body_parts = []
            if msg.is_multipart():
                for part in msg.walk():
                    body_parts.append(process_part(part))
            else:
                body_parts.append(process_part(msg))

            # Clean and truncate body
            cleaned_body = ' '.join(' '.join(body_parts).split())
            return cleaned_body[:MAX_BODY_LENGTH]

        except Exception as e:
            self.logger.error(f"Error extracting email body: {e}")
            return ""

    def process_email(self, msg: mailbox.mboxMessage) -> Optional[Dict[str, Any]]:
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

            return {
                "id": metadata.message_id,
                "subject": metadata.subject,
                "sender": metadata.sender,
                "recipients": metadata.recipients,
                "date": metadata.date_val,
                "body": body,
                "labels": metadata.labels,
                "thread_id": metadata.thread_id,
                **enrichments.__dict__
            }

        except Exception as e:
            self.logger.error(f"Error processing email: {e}")
            return None

    def _extract_metadata(self, msg: mailbox.mboxMessage) -> Optional[EmailMetadata]:
        """Extract and validate email metadata."""
        try:
            message_id = self.decode_header_value(msg.get('Message-ID', ''))
            subject = self.decode_header_value(msg.get('Subject', ''))
            sender = self.decode_header_value(msg.get('From', ''))
            recipients = self.decode_header_value(msg.get('To', ''))
            
            if not all([subject, sender, recipients]):
                return None
                
            date_str = self.decode_header_value(msg.get('Date', ''))
            date_val = parse_date(date_str) if date_str else int(time.time())
            
            return EmailMetadata(
                message_id=message_id or str(uuid.uuid4()),
                subject=subject,
                sender=sender,
                recipients=recipients,
                date_val=date_val,
                thread_id=self.decode_header_value(msg.get('Thread-Id', '')),
                labels=self._extract_labels(msg)
            )
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {e}")
            return None

    def _extract_labels(self, msg: mailbox.mboxMessage) -> List[str]:
        """Extract email labels with proper handling."""
        labels = self.decode_header_value(msg.get('X-Gmail-Labels', ''))
        return [label.strip() for label in labels.split(',')] if labels else []

    def _process_enrichments(self, body: str, message_id: str) -> Optional[EmailEnrichments]:
        """Process email enrichments with comprehensive error handling."""
        try:
            self.logger.info(f"[{message_id}] Processing enrichments...")
            
            return EmailEnrichments(
                body_vector=self._generate_embedding(body, message_id),
                entities=self._perform_ner(body, message_id),
                keywords=self._extract_keywords(body, message_id),
                summary=self._generate_summary(body, message_id)
            )
        except Exception as e:
            self.logger.error(f"[{message_id}] Failed to process enrichments: {e}")
            return None

    def _generate_embedding(self, text: str, message_id: str) -> List[float]:
        """Generate embedding vector with error handling and dimension verification."""
        try:
            vector = self.ml_models.embedding_model.encode(
                text, show_progress_bar=False
            ).tolist()
            
            # Verify vector dimensions
            if len(vector) != 1536:
                raise ValueError(f"Generated vector has {len(vector)} dimensions, expected 1536")
                
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

    def _generate_summary(self, text: str, message_id: str) -> str:
        """Generate text summary with error handling."""
        try:
            truncated_text = ' '.join(text.split()[:1024])
            summary = self.ml_models.summarizer(
                truncated_text,
                max_length=130,
                min_length=30,
                do_sample=False
            )
            return summary[0]['summary_text']
        except Exception as e:
            self.logger.error(f"[{message_id}] Failed to generate summary: {e}")
            return ""

    def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process batch of emails with retry mechanism."""
        if not batch:
            return

        batch_id = str(uuid.uuid4())[:8]
        self.logger.info(f"[Batch:{batch_id}] Processing batch of {len(batch)} documents")

        for attempt in range(MAX_RETRIES):
            try:
                response = self.typesense_client.collections['emails'].documents.import_(
                    batch, {'action': 'upsert'}
                )
                self._process_batch_response(response, batch, batch_id)
                return
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    self.logger.error(f"[Batch:{batch_id}] Failed after {MAX_RETRIES} attempts: {e}")
                    raise
                wait_time = 2 ** attempt
                self.logger.warning(f"[Batch:{batch_id}] Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)

    def _process_batch_response(self, response: List[Dict[str, Any]], 
                              batch: List[Dict[str, Any]], batch_id: str) -> None:
        """Process batch response with detailed logging."""
        success_count = sum(1 for r in response if r.get('success', False))
        error_count = len(response) - success_count
        
        self.logger.info(
            f"[Batch:{batch_id}] Results:\n"
            f"  Success: {success_count}/{len(batch)} ({success_count/len(batch)*100:.1f}%)\n"
            f"  Errors: {error_count}"
        )
        
        if error_count:
            for i, result in enumerate(response):
                if not result.get('success'):
                    self.logger.error(
                        f"[Batch:{batch_id}] Error indexing document {batch[i]['id']}: "
                        f"{result.get('error', 'Unknown error')}"
                    )

def setup_logging() -> logging.Logger:
    """Setup logging with rotation and formatting."""
    logger = logging.getLogger('EmailIndexer')
    logger.setLevel(logging.INFO)
    
    handler = RotatingFileHandler(
        "indexing.log",
        maxBytes=10**7,
        backupCount=5,
        encoding='utf-8'
    )
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

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

class TopicAnalyzer:
    """Handles topic modeling and temporal analysis."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.texts: List[str] = []
        self.dates: List[int] = []
        self.message_ids: List[str] = []

    def add_document(self, text: str, date: int, message_id: str) -> None:
        """Add a document for analysis."""
        self.texts.append(text)
        self.dates.append(date)
        self.message_ids.append(message_id)

    def perform_topic_modeling(self, num_topics: int = 10) -> List[Tuple[int, str]]:
        """Perform topic modeling with error handling."""
        try:
            self.logger.info("Starting topic modeling analysis...")
            
            # Tokenize and preprocess
            tokenized_texts = [text.lower().split() for text in self.texts]
            
            # Create dictionary and corpus
            dictionary = corpora.Dictionary(tokenized_texts)
            corpus = [dictionary.doc2bow(text) for text in tokenized_texts]
            
            # Build LDA model
            lda_model = models.LdaModel(
                corpus=corpus,
                id2word=dictionary,
                num_topics=num_topics,
                passes=15,
                random_state=42
            )
            
            topics = lda_model.show_topics(num_topics=num_topics)
            self.logger.info(f"Successfully identified {len(topics)} topics")
            return topics
            
        except Exception as e:
            self.logger.error(f"Error in topic modeling: {e}")
            return []

    def analyze_temporal_patterns(self) -> Dict[int, int]:
        """Analyze temporal patterns in email dates."""
        try:
            self.logger.info("Starting temporal analysis...")
            
            df = pd.DataFrame({'timestamp': self.dates})
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['hour'] = df['timestamp'].dt.hour
            
            patterns = df['hour'].value_counts().sort_index().to_dict()
            self.logger.info("Temporal analysis complete")
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error in temporal analysis: {e}")
            return {}

class EmailIndexer:
    """Main class for email indexing process."""
    
    def __init__(self, mbox_path: str):
        self.mbox_path = Path(mbox_path)
        self.logger = setup_logging()
        self.ml_models = MLModels(self.logger)
        self.topic_analyzer = TopicAnalyzer(self.logger)
        self.typesense_client = self._initialize_typesense()
        self.email_processor = None  # Initialize in run()

    def _initialize_typesense(self) -> typesense.Client:
        """Initialize Typesense client with retry logic."""
        api_key = os.environ.get('TYPESENSE_API_KEY', 'orion123')
        return typesense.Client({
            'nodes': [{
                'host': 'localhost',
                'port': '8108',
                'protocol': 'http'
            }],
            'api_key': api_key,
            'connection_timeout_seconds': 5,
            'retry_interval_seconds': 0.1
        })

    def _validate_environment(self) -> None:
        """Validate environment and requirements."""
        if not self.mbox_path.exists():
            raise FileNotFoundError(f"MBOX file not found at {self.mbox_path}")
        
        # Validate Typesense connection
        try:
            self.typesense_client.collections['emails'].retrieve()
        except Exception as e:
            self.logger.error(f"Failed to connect to Typesense: {e}")
            raise

    def _get_memory_usage(self) -> str:
        """Get current memory usage information."""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return f"Memory RSS: {memory_info.rss / 1024 / 1024:.2f}MB, VMS: {memory_info.vms / 1024 / 1024:.2f}MB"

    def process_emails(self) -> None:
        """Process emails with improved batch handling and observability."""
        try:
            self.logger.info(f"Opening mbox file at {self.mbox_path}")
            self.logger.info(f"Initial memory usage - {self._get_memory_usage()}")
            
            start_time = time.time()
            try:
                mbox = mailbox.mbox(str(self.mbox_path))
                load_time = time.time() - start_time
                self.logger.info(f"Successfully opened mbox file in {load_time:.2f} seconds")
            except Exception as e:
                self.logger.error(f"Failed to open mbox file: {e}")
                raise
            
            self.logger.info(f"Memory usage after opening mbox - {self._get_memory_usage()}")
            
            try:
                message_count = len(mbox)
                self.logger.info(f"Successfully counted {message_count} messages in mbox")
            except Exception as e:
                self.logger.error(f"Failed to count messages in mbox: {e}")
                raise
                
            self.logger.info(f"Memory usage after counting messages - {self._get_memory_usage()}")
            self.logger.info(f"Starting to process {message_count} messages from {self.mbox_path}")
            
            batch: List[Dict[str, Any]] = []
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_email = {}
                
                for i, msg in enumerate(mbox, start=1):
                    if i <= self.email_processor.processed_count:
                        continue
                        
                    future = executor.submit(self.email_processor.process_email, msg)
                    future_to_email[future] = i
                    
                    if len(future_to_email) >= BATCH_SIZE:
                        self._process_completed_futures(
                            future_to_email, batch, message_count
                        )
                
                # Process remaining futures
                self._process_completed_futures(future_to_email, batch, message_count)
                
                # Process final batch
                if batch:
                    self.email_processor.process_batch(batch)
                    self.email_processor._save_checkpoint()
            
            # Perform final analysis
            self._perform_final_analysis()
            
        except Exception as e:
            self.logger.error(f"Error processing emails: {e}")
            raise
        finally:
            self.ml_models.cleanup()

    def _process_completed_futures(
        self,
        future_to_email: Dict[Any, int],
        batch: List[Dict[str, Any]],
        message_count: int
    ) -> None:
        """Process completed futures with proper error handling."""
        for future in as_completed(future_to_email):
            try:
                result = future.result()
                if result:
                    batch.append(result)
                    self.email_processor.processed_count += 1
                    self.topic_analyzer.add_document(
                        result['body'],
                        result['date'],
                        result['id']
                    )
                    
                    if len(batch) >= BATCH_SIZE:
                        self.email_processor.process_batch(batch)
                        self.email_processor._save_checkpoint()
                        batch.clear()
                        gc.collect()
                    
                    if self.email_processor.processed_count % LOG_INTERVAL == 0:
                        self._log_progress(message_count)
                        
            except Exception as e:
                self.logger.error(f"Error processing future {future_to_email[future]}: {e}")
                self.email_processor.processed_count += 1
            
        future_to_email.clear()

    def _log_progress(self, message_count: int) -> None:
        """Log processing progress with detailed metrics."""
        progress = (self.email_processor.processed_count / message_count) * 100
        self.logger.info(
            f"Progress: {self.email_processor.processed_count}/{message_count} "
            f"messages ({progress:.1f}%)"
        )

    def _perform_final_analysis(self) -> None:
        """Perform final analysis with proper error handling."""
        try:
            if self.topic_analyzer.texts:
                self.logger.info("Performing final analysis...")
                
                # Topic modeling
                topics = self.topic_analyzer.perform_topic_modeling()
                if topics:
                    self.logger.info("Top topics identified:")
                    for topic_id, topic in topics:
                        self.logger.info(f"Topic {topic_id}: {topic}")
                
                # Temporal analysis
                patterns = self.topic_analyzer.analyze_temporal_patterns()
                if patterns:
                    self.logger.info("Temporal patterns identified:")
                    for hour, count in patterns.items():
                        self.logger.info(f"Hour {hour}: {count} emails")
                        
        except Exception as e:
            self.logger.error(f"Error in final analysis: {e}")

    def run(self) -> None:
        """Main execution method with proper initialization and cleanup."""
        try:
            self.logger.info("Starting email indexing process...")
            
            # Initialize environment
            self._validate_environment()
            
            # Initialize ML models
            self.ml_models.initialize()
            
            # Initialize email processor
            self.email_processor = EmailProcessor(
                self.logger,
                self.ml_models,
                self.typesense_client
            )
            
            # Process emails
            self.process_emails()
            
            self.logger.info("Email indexing process completed successfully")
            
        except Exception as e:
            self.logger.error(f"Fatal error in indexing process: {e}")
            raise
        finally:
            self.ml_models.cleanup()
            gc.collect()

def main():
    """Main entry point with proper error handling."""
    try:
        load_dotenv()
        indexer = EmailIndexer("../data/mbox_export.mbox")
        indexer.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()