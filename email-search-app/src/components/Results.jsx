// src/components/Results.jsx
import React from "react";
import { Card, CardContent, Typography, Chip, Grid } from "@mui/material";
import { format } from "date-fns";
import { Highlight } from "react-instantsearch-dom";

const Results = ({ hit }) => {
  // Determine if recipients is an array or a single string
  const recipients = Array.isArray(hit.recipients)
    ? hit.recipients.join(", ")
    : hit.recipients || "N/A"; // Fallback to 'N/A' if recipients is undefined or null

  return (
    <Grid item xs={12}>
      <Card variant="outlined">
        <CardContent>
          <Typography variant="h5">
            <Highlight attribute="subject" hit={hit} />
          </Typography>
          <Typography color="textSecondary">
            From: {hit.sender} | To: {recipients}
          </Typography>
          <Typography color="textSecondary">
            Date: {format(new Date(hit.date * 1000), "PPPpp")}
          </Typography>
          <div style={{ marginTop: "8px" }}>
            {hit.labels &&
              hit.labels.map((label) => (
                <Chip
                  key={label}
                  label={label}
                  style={{ marginRight: "4px" }}
                />
              ))}
          </div>
          <Typography variant="body2" style={{ marginTop: "8px" }}>
            <Highlight attribute="summary" hit={hit} />
          </Typography>
        </CardContent>
      </Card>
    </Grid>
  );
};

export default Results;
