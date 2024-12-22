// src/App.jsx
import React, { useState } from "react";
import {
  InstantSearch,
  Hits,
  Configure,
  Pagination,
} from "react-instantsearch-dom";
import {
  Container,
  Typography,
  CircularProgress,
  useMediaQuery,
  useTheme,
} from "@mui/material";
import SearchBar from "./components/SearchBar";
import Documentation from "./components/Documentation";
import Sidebar from "./components/Sidebar";
import searchClient from "./searchClient";
import { generateVector } from "./utils/generateVector";
import Results from "./components/Results";

const App = () => {
  const [customSearchParameters, setCustomSearchParameters] = useState({});
  const [loading, setLoading] = useState(false);
  const theme = useTheme();
  const isSmallScreen = useMediaQuery(theme.breakpoints.down("sm"));

  const handleSearch = async (query, vectorSearch) => {
    setLoading(true);
    try {
      if (vectorSearch) {
        const queryVector = await generateVector(query);
        setCustomSearchParameters({
          vector_query: {
            field: "body_vector",
            values: queryVector,
            k: 10, // Number of nearest neighbors
          },
        });
      } else {
        setCustomSearchParameters({});
      }
    } catch (error) {
      console.error("Vector generation error:", error);
      setCustomSearchParameters({});
      alert(
        "An error occurred while processing your search. Please try again."
      );
    }
    setLoading(false);
  };

  const Hit = ({ hit }) => <Results hit={hit} />;

  return (
    <InstantSearch searchClient={searchClient} indexName="emails">
      <Configure {...customSearchParameters} />
      <Sidebar />
      <Container
        maxWidth="lg"
        style={{
          marginTop: "32px",
          paddingLeft: isSmallScreen ? "0" : "240px",
          paddingRight: isSmallScreen ? "0" : "16px",
        }}
      >
        <Typography variant="h4" component="h1" gutterBottom>
          Email Search App
        </Typography>
        <SearchBar onSearch={handleSearch} />
        <Documentation />
        {loading && <CircularProgress style={{ marginTop: "16px" }} />}
        <Hits hitComponent={Hit} />
        <Pagination />
      </Container>
    </InstantSearch>
  );
};

export default App;
