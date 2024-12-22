<!-- App.svelte -->
<script>
  import { onMount } from 'svelte';
  import instantsearch from 'instantsearch.js';
  import {
    searchBox,
    hits,
    configure,
    pagination,
    refinementList,
    clearRefinements,
    stats,
    sortBy,
    hitsPerPage,
    rangeSlider
  } from 'instantsearch.js/es/widgets';
  import TypesenseInstantSearchAdapter from 'typesense-instantsearch-adapter';

  // State Variables
  let vectorMode = false;
  let showSuggestions = false;
  let showHelpModal = false;
  let suggestions = [
    'project deadlines',
    'invoice payment',
    'meeting notes',
    'body_vector:(urgent request k:10)',
    'body_vector:(follow up tomorrow k:5)'
  ];

  // Search examples for each mode
  const searchExamples = {
    normal: [
      { query: 'urgent meeting', description: 'Find emails about urgent meetings' },
      { query: 'from:john@example.com', description: 'Search emails from a specific sender' },
      { query: 'subject:"project update"', description: 'Search in subject line' },
    ],
    vector: [
      { query: 'follow up on project deadlines', description: 'Find semantically similar content about project deadlines' },
      { query: 'meeting preparation documents', description: 'Find content about preparing for meetings' },
      { query: 'body_vector:(urgent request k:10)', description: 'Advanced: Direct vector query with k nearest neighbors' },
    ]
  };

  // Tooltips
  const queryTooltip = `Enter your search terms.
- Normal mode: keywords are matched against subject and body fields
- Vector mode: uses semantic embeddings to find similar content`;

  const modeTooltip = `Choose your search mode:
- Normal: Traditional keyword-based search
- Vector: Semantic search using AI embeddings`;

  // Typesense Configuration
  const apiKey = 'orion123';
  const typesenseHost = 'localhost'; 
  const collectionName = 'emails';

  let searchInstance;
  let query = '';

  // Function to Initialize InstantSearch
  function initializeSearch(vectorMode) {
    console.log('[initializeSearch] Initializing search with vectorMode:', vectorMode);

    if (searchInstance) {
      console.log('[initializeSearch] Disposing previous search instance');
      searchInstance.dispose();
    }

    let additionalSearchParameters = {
      highlight_full_fields: 'body',
      highlight_affix_num_tokens: 50
    };

    if (vectorMode) {
      additionalSearchParameters.query_by = 'body';
      additionalSearchParameters.vector_query = '';
    } else {
      additionalSearchParameters.query_by = 'subject,body';
    }

    const typesenseInstantsearchAdapter = new TypesenseInstantSearchAdapter({
      server: {
        apiKey: apiKey,
        nodes: [
          {
            host: typesenseHost,
            port: 8108,
            protocol: 'http'
          }
        ],
        connectionTimeoutSeconds: 2
      },
      additionalSearchParameters
    });

    const searchClient = typesenseInstantsearchAdapter.searchClient;

    searchInstance = instantsearch({
      indexName: collectionName,
      searchClient,
      searchFunction(helper) {
        const query = helper.state.query;
        if (query === '') {
          document.querySelector('#hits').innerHTML = '<div class="no-results">Enter a search query to see results</div>';
          document.querySelector('#pagination').innerHTML = '';
          return;
        }
        helper.search();
      },
      future: {
        preserveSharedStateOnUnmount: true
      }
    });

    searchInstance.addWidgets([
      // Search Box
      searchBox({
        container: '#searchbox',
        showSubmit: false,
        showReset: true,
        placeholder: 'Search emails...',
        cssClasses: {
          input: 'search-input',
          root: 'search-box'
        },
        onUpdate({ query: newQuery }) {
          console.log('[searchBox] Query updated:', newQuery);
          query = newQuery;

          if (vectorMode) {
            if (newQuery.trim() !== '') {
              const updatedVectorQuery = `body_vector:(${newQuery} k:10)`;
              searchInstance.helper.setQuery('').setQueryParameters({
                vector_query: updatedVectorQuery
              }).search();
            } else {
              searchInstance.helper.setQuery('').setQueryParameters({
                vector_query: ''
              }).search();
            }
          } else {
            searchInstance.helper.setQuery(newQuery).search();
          }
        }
      }),

      // Clear Refinements
      clearRefinements({
        container: '#clear-refinements',
        templates: {
          resetLabel: 'Clear All Filters'
        },
        cssClasses: {
          reset: 'clear-filters-button'
        }
      }),

      // Stats
      stats({
        container: '#stats',
        templates: {
          body: `
            <div class="stats-container">
              <span>{{#helpers.formatNumber}}{{nbHits}}{{/helpers.formatNumber}} results found</span>
              <span>in {{processingTimeMS}}ms</span>
            </div>
          `
        },
        cssClasses: {
          root: 'stats'
        }
      }),

      // Refinement Lists
      refinementList({
        container: '#refinement-sender',
        attribute: 'sender',
        searchable: true,
        searchablePlaceholder: 'Search senders',
        showMore: true,
        limit: 5,
        templates: {
          header: '<h4>Sender</h4>'
        }
      }),
      refinementList({
        container: '#refinement-recipients',
        attribute: 'recipients',
        searchable: true,
        searchablePlaceholder: 'Search recipients',
        showMore: true,
        limit: 5,
        templates: {
          header: '<h4>Recipients</h4>'
        }
      }),
      refinementList({
        container: '#refinement-intent',
        attribute: 'intent',
        searchable: true,
        searchablePlaceholder: 'Search intents',
        showMore: true,
        limit: 5,
        templates: {
          header: '<h4>Intent</h4>'
        }
      }),
      refinementList({
        container: '#refinement-labels',
        attribute: 'labels',
        searchable: true,
        searchablePlaceholder: 'Search labels',
        showMore: true,
        limit: 5,
        templates: {
          header: '<h4>Labels</h4>'
        }
      }),

      // Range Slider for Date
      rangeSlider({
        container: '#range-slider-date',
        attribute: 'date',
        tooltips: {
          format(rawValue) {
            const date = new Date(rawValue * 1000);
            return date.toLocaleDateString();
          }
        },
        templates: {
          header: '<h4>Date</h4>'
        }
      }),

      // Hits Per Page
      hitsPerPage({
        container: '#hits-per-page',
        items: [
          { label: '10 hits per page', value: 10, default: true },
          { label: '20 hits per page', value: 20 },
          { label: '50 hits per page', value: 50 }
        ],
        cssClasses: {
          select: 'hits-per-page-select'
        }
      }),

      // Sort By
      sortBy({
        container: '#sort-by',
        items: [
          { label: 'Most Recent', value: `${collectionName}_desc` },
          { label: 'Oldest', value: `${collectionName}_asc` },
          { label: 'Relevance', value: `${collectionName}` }
        ],
        cssClasses: {
          select: 'sort-by-select'
        }
      }),

      // Hits
      hits({
        container: '#hits',
        templates: {
          empty: `<div class="no-results">No results found. Try a different query.</div>`,
          item(hit) {
            const subject = hit.subject || 'No Subject';
            const sender = hit.sender || 'Unknown Sender';
            const date = hit.date ? new Date(hit.date * 1000).toLocaleString() : 'Unknown Date';
            let snippet = '';

            if (hit._highlightResult && hit._highlightResult.body) {
              snippet = typeof hit._highlightResult.body.value === 'string'
                ? hit._highlightResult.body.value
                : 'No snippet available';
            } else if (hit.body) {
              snippet = hit.body.slice(0, 200) + '...';
            } else {
              snippet = 'No content available';
            }

            return `
              <div class="result-card">
                <h3 class="result-subject">${subject}</h3>
                <div class="result-meta">
                  <span>${sender}</span>
                  <span class="meta-separator">•</span>
                  <span>${date}</span>
                </div>
                <div class="result-snippet">${snippet}</div>
              </div>
            `;
          }
        }
      }),

      // Pagination
      pagination({
        container: '#pagination',
        padding: 2,
        cssClasses: {
          root: 'pagination',
          item: 'page-item',
          link: 'page-link',
          selectedItem: 'active',
          disabledItem: 'disabled'
        }
      }),

      // Hits Per Page Configuration
      configure({
        hitsPerPage: 10
      })
    ]);

    searchInstance.start();
  }

  onMount(() => {
    const searchbox = document.getElementById('searchbox');
    if (searchbox) {
      console.log('[onMount] searchbox found, initializing search');
      initializeSearch(vectorMode);
    } else {
      console.error('[onMount] searchbox not found');
    }
  });

  function handleVectorModeChange(event) {
    vectorMode = event.target.checked;
    initializeSearch(vectorMode);
  }

  function applySuggestion(s) {
    const searchboxInput = document.querySelector('#searchbox input');
    if (searchboxInput) {
      searchboxInput.value = s;
      query = s;
      if (vectorMode && s.trim() !== '') {
        const updatedVectorQuery = `body_vector:(${s} k:10)`;
        searchInstance.helper.setQuery('').setQueryParameters({
          vector_query: updatedVectorQuery
        }).search();
      } else if (!vectorMode) {
        searchInstance.helper.setQuery(s).search();
      }
    }
    showSuggestions = false;
  }

  function handleInput(event) {
    showSuggestions = event.target.value.length > 0;
  }

  function triggerSearch() {
    if (searchInstance) {
      searchInstance.helper.search();
    }
  }
</script>

<style>
  :global(body) {
    margin: 0;
    font-family: 'Inter', sans-serif;
    background-color: #f9fafb;
    color: #1f2937;
  }

  .app-container {
    min-height: 100vh;
    display: flex;
    flex-direction: column;
  }

  /* Header Styles */
  .header {
    background-color: #ffffff;
    border-bottom: 1px solid #e5e7eb;
    padding: 1rem 0;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  }

  .header-content {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 1.5rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .header h1 {
    font-size: 1.75rem;
    font-weight: 700;
    color: #111827;
    margin: 0;
  }

  .help-button {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border: 1px solid #d1d5db;
    background-color: #f3f4f6;
    border-radius: 0.375rem;
    color: #4b5563;
    cursor: pointer;
    font-size: 0.875rem;
    transition: background-color 0.2s, border-color 0.2s, color 0.2s;
  }

  .help-button:hover {
    background-color: #e5e7eb;
    border-color: #9ca3af;
    color: #111827;
  }

  /* Main Content Styles */
  .main-content {
    max-width: 1200px;
    margin: 2rem auto;
    padding: 0 1.5rem;
    flex: 1;
    display: flex;
    gap: 2rem;
  }

  /* Sidebar Styles */
  .sidebar {
    width: 280px;
    background-color: #ffffff;
    border-radius: 0.5rem;
    padding: 1.5rem;
    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
    height: fit-content;
    position: sticky;
    top: 2rem;
  }

  /* Search Container Styles */
  .search-container {
    flex: 1;
  }

  .search-controls {
    display: flex;
    gap: 1rem;
    align-items: center;
    flex-wrap: wrap;
    margin-bottom: 1.5rem;
  }

  .search-box {
    flex: 1;
    min-width: 300px;
    position: relative;
    max-width: 600px;
  }

  .search-input {
    width: 100%;
    padding: 0.75rem 1rem 0.75rem 2.5rem;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    font-size: 1rem;
    background-color: #ffffff;
    transition: border-color 0.2s, box-shadow 0.2s;
  }

  .search-input:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
  }

  .search-icon {
    position: absolute;
    left: 0.75rem;
    top: 50%;
    transform: translateY(-50%);
    color: #9ca3af;
    pointer-events: none;
  }

  /* Mode Toggle Styles */
  .mode-toggle {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.5rem 1rem;
    background-color: #f9fafb;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    font-size: 0.875rem;
    cursor: pointer;
    transition: background-color 0.2s, border-color 0.2s;
  }

  .mode-toggle:hover {
    background-color: #f3f4f6;
    border-color: #9ca3af;
  }

  .mode-toggle input {
    width: 16px;
    height: 16px;
    cursor: pointer;
  }

  /* Search Button Styles */
  .search-button {
    padding: 0.75rem 1.5rem;
    background-color: #3b82f6;
    color: #ffffff;
    border: none;
    border-radius: 0.375rem;
    font-weight: 600;
    cursor: pointer;
    font-size: 0.875rem;
    transition: background-color 0.2s, transform 0.1s;
  }

  .search-button:hover {
    background-color: #2563eb;
  }

  .search-button:active {
    transform: scale(0.98);
  }

  /* Sidebar Widgets */
  .sidebar-widget {
    margin-bottom: 1.75rem;
  }

  .sidebar-widget h4 {
    margin-bottom: 0.75rem;
    font-size: 1.125rem;
    color: #111827;
    border-bottom: 1px solid #d1d5db;
    padding-bottom: 0.5rem;
  }

  /* Clear Refinements Button */
  .clear-filters-button {
    padding: 0.5rem 1rem;
    background-color: #ef4444;
    color: #ffffff;
    border: none;
    border-radius: 0.375rem;
    cursor: pointer;
    font-size: 0.875rem;
    transition: background-color 0.2s, transform 0.1s;
  }

  .clear-filters-button:hover {
    background-color: #dc2626;
  }

  .clear-filters-button:active {
    transform: scale(0.98);
  }

  /* Stats Styles */
  .stats-container {
    display: flex;
    gap: 1.5rem;
    font-size: 0.875rem;
    color: #6b7280;
    align-items: center;
  }

  /* Hits Per Page Select */
  .hits-per-page-select {
    padding: 0.5rem;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    background-color: #ffffff;
    cursor: pointer;
    font-size: 0.875rem;
    transition: border-color 0.2s, box-shadow 0.2s;
  }

  .hits-per-page-select:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
  }

  /* Sort By Select */
  .sort-by-select {
    padding: 0.5rem;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    background-color: #ffffff;
    cursor: pointer;
    font-size: 0.875rem;
    transition: border-color 0.2s, box-shadow 0.2s;
  }

  .sort-by-select:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
  }

  /* Results Styles */
  .result-card {
    padding: 1.75rem;
    border-bottom: 1px solid #e5e7eb;
    background-color: #ffffff;
    border-radius: 0.375rem;
    margin-bottom: 1rem;
    box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    transition: transform 0.2s, box-shadow 0.2s;
  }

  .result-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
  }

  .result-card:last-child {
    border-bottom: none;
  }

  .result-subject {
    font-size: 1.25rem;
    font-weight: 600;
    color: #111827;
    margin: 0 0 0.5rem 0;
  }

  .result-meta {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    color: #6b7280;
    font-size: 0.875rem;
    margin-bottom: 0.75rem;
  }

  .meta-separator {
    color: #9ca3af;
  }

  .result-snippet {
    color: #4b5563;
    font-size: 0.875rem;
    line-height: 1.6;
  }

  /* Pagination Styles */
  :global(.pagination) {
    display: flex !important;
    justify-content: center;
    gap: 0.5rem;
    margin-top: 2rem;
    list-style: none;
    padding: 0;
  }

  :global(.pagination .page-item) {
    display: inline-flex;
  }

  :global(.pagination .page-link) {
    padding: 0.5rem 0.75rem;
    border: 1px solid #d1d5db;
    background-color: #ffffff;
    color: #4b5563;
    font-size: 0.875rem;
    cursor: pointer;
    transition: all 0.2s;
    text-decoration: none;
    border-radius: 0.375rem;
    min-width: 2.5rem;
    text-align: center;
  }

  :global(.pagination .page-link:hover) {
    background-color: #f3f4f6;
    border-color: #9ca3af;
  }

  :global(.pagination .active .page-link) {
    background-color: #3b82f6;
    border-color: #3b82f6;
    color: #ffffff;
  }

  :global(.pagination .disabled .page-link) {
    background-color: #f9fafb;
    border-color: #e5e7eb;
    color: #9ca3af;
    cursor: not-allowed;
  }

  /* Controls Container */
  .controls {
    display: flex;
    gap: 1rem;
    align-items: center;
    margin-bottom: 1.5rem;
    flex-wrap: wrap;
  }

  /* Help Modal Styles */
  .help-modal {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background-color: rgba(31, 41, 55, 0.8);
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 1000;
    padding: 1rem;
  }

  .modal-content {
    background-color: #ffffff;
    padding: 2rem;
    border-radius: 0.5rem;
    max-width: 700px;
    width: 100%;
    max-height: 90vh;
    overflow-y: auto;
    position: relative;
    box-shadow: 0 10px 25px rgba(0,0,0,0.1);
    animation: fadeIn 0.3s ease-out;
  }

  .modal-close {
    position: absolute;
    top: 1rem;
    right: 1rem;
    background: none;
    border: none;
    color: #6b7280;
    cursor: pointer;
    padding: 0.5rem;
    font-size: 1.25rem;
    transition: color 0.2s;
  }

  .modal-close:hover {
    color: #111827;
  }

  .modal-content h2 {
    margin-top: 0;
    font-size: 1.75rem;
    color: #111827;
  }

  .modal-content h3 {
    font-size: 1.25rem;
    color: #111827;
    margin-top: 1.5rem;
  }

  .modal-content p,
  .modal-content ul {
    font-size: 1rem;
    color: #4b5563;
    line-height: 1.6;
  }

  .modal-content code {
    background-color: #f3f4f6;
    padding: 0.2rem 0.4rem;
    border-radius: 0.25rem;
    font-family: 'Courier New', Courier, monospace;
  }

  /* Search Examples Styles */
  .search-examples {
    margin-top: 1.5rem;
    padding: 1.5rem;
    background-color: #f3f4f6;
    border-radius: 0.375rem;
    border: 1px solid #d1d5db;
  }

  .search-examples h3 {
    margin-bottom: 1rem;
    font-size: 1.25rem;
    color: #111827;
  }

  .example-list {
    margin-top: 0.75rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .example-item {
    display: flex;
    align-items: flex-start;
    gap: 1rem;
    padding: 0.75rem;
    background-color: #ffffff;
    border-radius: 0.375rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    transition: background-color 0.2s, box-shadow 0.2s;
  }

  .example-item:hover {
    background-color: #f9fafb;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  }

  .example-query {
    background-color: #e5e7eb;
    padding: 0.3rem 0.6rem;
    border-radius: 0.25rem;
    font-family: 'Fira Code', monospace;
    font-size: 0.875rem;
    color: #111827;
    flex-shrink: 0;
  }

  .example-description {
    color: #6b7280;
    font-size: 0.875rem;
    flex: 1;
  }

  /* Suggestions Styles */
  .suggestions-container {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    margin-top: 0.25rem;
    background-color: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    z-index: 50;
    max-height: 200px;
    overflow-y: auto;
  }

  .suggestion-item {
    padding: 0.75rem 1rem;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .suggestion-item:hover {
    background-color: #f3f4f6;
  }

  /* No Results Styles */
  .no-results {
    text-align: center;
    padding: 3rem 1rem;
    color: #6b7280;
    font-style: italic;
    font-size: 1rem;
  }

  /* Tooltip Styles */
  .tooltip {
    position: relative;
    display: inline-block;
  }

  .tooltip:hover .tooltip-text {
    display: block;
  }

  .tooltip-text {
    display: none;
    position: absolute;
    bottom: 125%;
    left: 50%;
    transform: translateX(-50%);
    padding: 0.75rem;
    background-color: #1f2937;
    color: #ffffff;
    font-size: 0.75rem;
    border-radius: 0.375rem;
    white-space: pre-line;
    width: max-content;
    max-width: 300px;
    margin-bottom: 0.5rem;
    z-index: 50;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
  }

  /* Responsive Styles */
  @media (max-width: 1024px) {
    .sidebar {
      display: none;
    }
    .main-content {
      flex-direction: column;
    }
  }

  @media (max-width: 640px) {
    .search-controls {
      flex-direction: column;
    }

    .search-box {
      width: 100%;
    }

    .mode-toggle,
    .search-button {
      width: 100%;
      justify-content: center;
    }

    .controls {
      flex-direction: column;
      align-items: stretch;
    }

    .hits-per-page-select,
    .sort-by-select {
      width: 100%;
    }
  }

  /* Animation */
  @keyframes fadeIn {
    from {
      opacity: 0;
      transform: scale(0.95);
    }
    to {
      opacity: 1;
      transform: scale(1);
    }
  }
</style>

<div class="app-container">
  <header class="header">
    <div class="header-content">
      <h1>Email Search</h1>
      <button class="help-button" on:click={() => showHelpModal = true}>
        <span>Help</span>
      </button>
    </div>
  </header>

  <main class="main-content">
    <!-- Sidebar for Filters -->
    <aside class="sidebar">
      <div id="clear-refinements" class="sidebar-widget"></div>
      <div id="stats" class="sidebar-widget"></div>
      <div id="refinement-sender" class="sidebar-widget"></div>
      <div id="refinement-recipients" class="sidebar-widget"></div>
      <div id="refinement-intent" class="sidebar-widget"></div>
      <div id="refinement-labels" class="sidebar-widget"></div>
      <div id="range-slider-date" class="sidebar-widget"></div>
    </aside>

    <div class="search-container">
      <div class="search-controls">
        <div class="search-box" on:input={handleInput}>
          <div id="searchbox"></div>
          {#if showSuggestions && query && query.length > 0}
            <div class="suggestions-container">
              {#each suggestions.filter(s => s.toLowerCase().includes(query.toLowerCase())) as s}
                <div class="suggestion-item" on:click={() => applySuggestion(s)}>
                  {s}
                </div>
              {/each}
            </div>
          {/if}
        </div>

        <div class="mode-toggle tooltip">
          <input
            type="checkbox"
            bind:checked={vectorMode}
            on:change={handleVectorModeChange}
            id="mode-toggle"
          >
          <label for="mode-toggle">{vectorMode ? 'Vector Mode' : 'Normal Mode'}</label>
          <div class="tooltip-text">{modeTooltip}</div>
        </div>

        <button class="search-button" on:click={triggerSearch}>
          Search
        </button>
      </div>

      <div class="controls">
        <div id="hits-per-page" class="sidebar-widget"></div>
        <div id="sort-by" class="sidebar-widget"></div>
      </div>

      <div class="search-examples">
        <h3>Search Examples - {vectorMode ? 'Vector Mode' : 'Normal Mode'}</h3>
        <div class="example-list">
          {#each searchExamples[vectorMode ? 'vector' : 'normal'] as example}
            <div class="example-item">
              <code class="example-query">{example.query}</code>
              <span class="example-description">{example.description}</span>
            </div>
          {/each}
        </div>
      </div>

      <div id="hits"></div>
      <div id="pagination"></div>
    </div>
  </main>
</div>

{#if showHelpModal}
  <div class="help-modal" on:click={() => showHelpModal = false}>
    <div class="modal-content" on:click|stopPropagation>
      <button class="modal-close" on:click={() => showHelpModal = false}>×</button>
      <h2>Search Help</h2>
      <div>
        <h3>Search Modes</h3>
        <p><strong>Normal Mode:</strong> Traditional keyword-based search that looks for exact matches in the subject and body of emails.</p>
        <p><strong>Vector Mode:</strong> Semantic search that understands the meaning of your query and finds related content, even if the exact words don't match.</p>

        <h3>Search Syntax</h3>
        <ul>
          <li><code>from:email@example.com</code> - Search by sender</li>
          <li><code>subject:"meeting notes"</code> - Search in subject line</li>
          <li><code>body_vector:(query k:10)</code> - Advanced vector search with k nearest neighbors</li>
        </ul>

        <h3>Tips</h3>
        <ul>
          <li>Use quotes for exact phrases: <code>"project deadline"</code></li>
          <li>Vector mode is better for finding conceptually similar content</li>
          <li>Normal mode is better for finding specific keywords or phrases</li>
        </ul>
      </div>
    </div>
  </div>
{/if}
