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

# Example vector query: search for emails related to "project deadlines"
search_parameters = {
    'q': '',  # q can be empty or optional here since we're using vector_query
    'query_by': 'body',  # still required, helps Typesense know what field text to embed at query time
    'vector_query': 'body_vector:(project deadlines k:10)'
    # This tells Typesense: "Embed 'project deadlines' and find top 10 nearest vectors"
}

results = client.collections['emails'].documents.search(search_parameters)

print("Found:", results['found'], "results")
for hit in results['hits']:
    doc = hit['document']
    print("Subject:", doc['subject'], "Sender:", doc['sender'], "Date:", doc['date'])
