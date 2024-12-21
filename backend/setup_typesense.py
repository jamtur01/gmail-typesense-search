import typesense

client = typesense.Client({
    'nodes': [{
        'host': 'localhost',
        'port': '8108',
        'protocol': 'http'
    }],
    'api_key': 'orion123',  # Replace with your actual Typesense API key
    'connection_timeout_seconds': 2
})

# Optional: Delete the collection if you need to recreate it
try:
    client.collections['emails'].delete()
    print("Deleted existing 'emails' collection.")
except typesense.exceptions.TypesenseClientError as e:
    if e.code == 'c4006':
        print("Collection 'emails' does not exist. Proceeding to create a new one.")
    else:
        raise e

schema = {
    "name": "emails",
    "fields": [
        {"name": "id", "type": "string"},  # Unique identifier set to message_id
        {"name": "subject", "type": "string"},
        {"name": "sender", "type": "string", "facet": True},
        {"name": "recipients", "type": "string", "facet": True},
        {"name": "date", "type": "int64", "facet": True},
        {"name": "body", "type": "string"},
        {"name": "labels", "type": "string[]", "facet": True},  # Array of labels
        {"name": "thread_id", "type": "string"},  # Thread ID
        {"name": "sentiment", "type": "string", "facet": True},  # Sentiment category
        {
            "name": "body_vector",
            "type": "float[]",
            "num_dim": 1536,
            "embedding_source": "body",
            "embedding_model": "openai_text_embedding_ada_002",
            "optional": True
        }
    ],
    "default_sorting_field": "date"
}

try:
    client.collections.create(schema)
    print("Collection 'emails' with vector field created successfully.")
except typesense.exceptions.TypesenseClientError as e:
    print(f"Failed to create collection: {e}")
