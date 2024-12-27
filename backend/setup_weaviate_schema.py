import weaviate
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the Weaviate URL from environment variables or use default
WEAVIATE_URL = os.getenv('WEAVIATE_URL', 'http://localhost:8080')

def create_weaviate_schema():
    client = None  # Initialize client variable
    try:
        # Initialize the Weaviate client using WeaviateClient
        client = weaviate.connect_to_local()
        
        # Check if the client is ready
        if not client.is_ready():
            print("Weaviate instance is not ready.")
            return
        
         # Create the Email collection
        client.collections.create(
            "Email",
            properties=[
                Property(name="title", data_type=DataType.TEXT),
                Property(name="sender", data_type=DataType.TEXT),
                Property(name="recipients", data_type=DataType.TEXT_ARRAY),
                Property(name="date_val", data_type=DataType.DATE),
                Property(name="thread_id", data_type=DataType.TEXT),
                Property(name="labels", data_type=DataType.TEXT_ARRAY),
                Property(name="body", data_type=DataType.TEXT),
                Property(name="entities", data_type=DataType.TEXT_ARRAY),
                Property(name="keywords", data_type=DataType.TEXT_ARRAY),
            ]
        )
        print("Weaviate schema created successfully.")
    
    except Exception as e:
        print(f"Failed to create Weaviate schema: {e}")
    
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env
    create_weaviate_schema()