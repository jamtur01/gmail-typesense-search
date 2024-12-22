// src/components/SearchBar.jsx
import React, { useState, useEffect } from 'react';
import {
  TextField,
  IconButton,
  Tooltip,
  Button,
  FormControlLabel,
  Switch,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import InfoIcon from '@mui/icons-material/Info';
import { debounce } from 'lodash';

const SearchBar = ({ onSearch }) => {
  const [query, setQuery] = useState('');
  const [vectorSearch, setVectorSearch] = useState(false);

  // Debounce the search function to prevent excessive calls
  const debouncedSearch = React.useRef(
    debounce((q, vs) => {
      onSearch(q, vs);
    }, 300)
  ).current;

  useEffect(() => {
    return () => {
      debouncedSearch.cancel();
    };
  }, [debouncedSearch]);

  const handleSearch = () => {
    debouncedSearch(query, vectorSearch);
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleExample = () => {
    const exampleQuery = 'subject:"Project Update"';
    setQuery(exampleQuery);
    onSearch(exampleQuery, vectorSearch);
  };

  return (
    <div style={styles.container}>
      <TextField
        label="Search Emails"
        variant="outlined"
        fullWidth
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyPress={handleKeyPress}
        helperText="Search by subject, sender, recipients, or body."
      />
      <IconButton onClick={handleSearch} color="primary" aria-label="search">
        <SearchIcon />
      </IconButton>
      <Tooltip
        title="Enter your search query. Use syntax like 'subject:Meeting' or 'sender:john@example.com'."
        arrow
      >
        <IconButton aria-label="info">
          <InfoIcon color="action" />
        </IconButton>
      </Tooltip>
      <Button variant="text" onClick={handleExample}>
        Example
      </Button>
      <FormControlLabel
        control={
          <Switch
            checked={vectorSearch}
            onChange={(e) => setVectorSearch(e.target.checked)}
            color="primary"
            inputProps={{ 'aria-label': 'vector search toggle' }}
          />
        }
        label="Vector Search"
      />
    </div>
  );
};

const styles = {
  container: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    flexWrap: 'wrap',
    margin: '16px 0',
  },
};

export default SearchBar;

