import typesense

client = typesense.Client({
  'nodes': [{
    'host': 'localhost',
    'port': '8108',
    'protocol': 'http'
  }],
  'api_key': 'orion123',
  'connection_timeout_seconds': 2
})

try:
    client.collections['emails'].delete()
except:
    pass

schema = {
    "name": "emails",
    "fields": [
        {"name": "message_id", "type": "string"},
        {"name": "subject", "type": "string"},
        {"name": "sender", "type": "string", "facet": True},
        {"name": "recipients", "type": "string", "facet": True},
        {"name": "date", "type": "int64", "facet": True},
        {"name": "body", "type": "string"},
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

client.collections.create(schema)
print("Collection 'emails' with vector field created.")
