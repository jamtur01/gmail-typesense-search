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

search_parameters = {
    'q': 'some search term',
    'query_by': 'subject,body'  # Fields to search
}

results = client.collections['emails'].documents.search(search_parameters)
print("Found:", results['found'], "results")
for hit in results['hits']:
    doc = hit['document']
    print("Subject:", doc['subject'], "Sender:", doc['sender'], "Date:", doc['date'])
