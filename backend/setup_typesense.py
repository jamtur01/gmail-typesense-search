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

#client.collections['emails'].delete()
#print("Deleted existing 'emails' collection.")

schema = {
    "name": "emails",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "subject", "type": "string"},
        {"name": "sender", "type": "string", "facet": True},
        {"name": "recipients", "type": "string", "facet": True},
        {"name": "date", "type": "int64", "facet": True, "sort": True},
        {"name": "body", "type": "string"},
        {"name": "labels", "type": "string[]", "facet": True, "optional": True},
        {"name": "thread_id", "type": "string", "optional": True},
        {
            "name": "body_vector",
            "type": "float[]",
            "num_dim": 1536,
            "optional": True,
            "hnsw_params": {
                "M": 16,
                "ef_construction": 200
            },
            "vec_dist": "cosine"
        },
        {"name": "entities", "type": "object[]", "optional": True},  # Changed to object[]
        {"name": "keywords", "type": "string[]", "optional": True},
        {"name": "summary", "type": "string", "optional": True},
        {"name": "intent", "type": "string", "facet": True, "optional": True}
    ],
    "default_sorting_field": "date",
    "enable_nested_fields": True  # Added for object[] support
}

try:
    client.collections.create(schema)
    print("Collection 'emails' with vector field created successfully.")
except typesense.exceptions.TypesenseClientError as e:
    print(f"Failed to create collection: {e}")
