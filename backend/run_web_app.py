from flask import Flask, request, render_template_string
import typesense

client = typesense.Client({
  'nodes': [{'host': 'localhost', 'port': '8108', 'protocol': 'http'}],
  'api_key': 'orion123',
  'connection_timeout_seconds': 2
})

HTML_TEMPLATE = """
<!doctype html>
<html>
<head><title>Email Search</title></head>
<body>
    <h1>Search Emails</h1>
    <form method="get">
        <input type="text" name="q" value="{{ query|e }}">
        <input type="submit" value="Search">
    </form>
    <hr>
    {% if results %}
    <h2>Results ({{ count }} found)</h2>
    <ul>
    {% for r in results %}
        <li>
          <strong>Subject:</strong> {{ r.document.subject }}<br>
          <strong>From:</strong> {{ r.document.sender }}<br>
          <strong>Date:</strong> {{ r.document.date }}<br>
          <strong>Body snippet:</strong> {{ r.highlight.body }}
        </li>
    {% endfor %}
    </ul>
    {% endif %}
</body>
</html>
"""

app = Flask(__name__)

@app.route("/")
def search():
    query_str = request.args.get("q", "")
    results_data = None
    count = 0
    if query_str:
        search_parameters = {
            'q': query_str,
            'query_by': 'subject,body',
            'highlight_full_fields': 'body',
            'highlight_affix_num_tokens': 5
        }
        res = client.collections['emails'].documents.search(search_parameters)
        count = res['found']
        results_data = res['hits']

    return render_template_string(HTML_TEMPLATE, query=query_str, results=results_data, count=count)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
