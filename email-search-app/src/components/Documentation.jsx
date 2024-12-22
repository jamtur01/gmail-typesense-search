// src/components/Documentation.jsx
import React from 'react';
import {
  Typography,
  Tooltip,
  IconButton,
} from '@mui/material';
import HelpIcon from '@mui/icons-material/Help';

const Documentation = () => {
  return (
    <div style={styles.container}>
      <Typography variant="h6">
        Search Documentation
        <Tooltip
          title="Use field-specific queries like 'subject:Meeting' or 'sender:john@example.com'. Use quotes for exact matches."
          arrow
        >
          <IconButton size="small">
            <HelpIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Typography>
      <Typography variant="body2" style={{ marginTop: '8px' }}>
        <strong>Examples:</strong>
        <ul>
          <li>
            <code>subject:"Project Update"</code>
          </li>
          <li>
            <code>sender:alice@example.com</code>
          </li>
          <li>
            <code>
              labels:important AND date:&gt;1609459200
            </code>{' '}
            (emails labeled important and after Jan 1, 2021)
          </li>
        </ul>
      </Typography>
    </div>
  );
};

const styles = {
  container: {
    marginTop: '16px',
    padding: '16px',
    border: '1px solid #ddd',
    borderRadius: '8px',
    backgroundColor: '#f9f9f9',
  },
};

export default Documentation;

