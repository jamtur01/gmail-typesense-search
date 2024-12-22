// src/searchClient.js
import TypesenseInstantSearchAdapter from 'typesense-instantsearch-adapter';

const typesenseInstantsearchAdapter = new TypesenseInstantSearchAdapter({
  server: {
    apiKey: import.meta.env.VITE_TYPESENSE_API_KEY, // Securely using environment variable
    nodes: [
      {
        host: 'localhost',
        port: '8108',
        protocol: 'http',
      },
    ],
    cacheSearchResultsForSeconds: 60, // Cache search results for 60 seconds
  },
  additionalSearchParameters: {
    query_by: 'subject,sender,recipients,body',
    sort_by: 'date:desc',
  },
});

const searchClient = typesenseInstantsearchAdapter.searchClient;

export default searchClient;

