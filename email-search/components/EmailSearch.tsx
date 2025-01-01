// EmailSearch.tsx
import React, { useState } from "react";
import { EmailSearchApi } from "@/lib/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const WEAVIATE_URL = process.env.NEXT_PUBLIC_WEAVIATE_URL || "localhost:8080";
const OPENAI_KEY = process.env.NEXT_PUBLIC_OPENAI_API_KEY || "";

export const EmailSearch: React.FC = () => {
  const [searchApi] = useState(
    () => new EmailSearchApi(WEAVIATE_URL, OPENAI_KEY)
  );
  const [query, setQuery] = useState("");
  const [searchMode, setSearchMode] = useState<"semantic" | "hybrid">("hybrid");
  const [results, setResults] = useState<Email[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<SearchFilters>({});

  const handleSearch = async () => {
    if (!query.trim()) return;

    setLoading(true);
    setError(null);

    try {
      // Generate enhanced search query using OpenAI
      const enhancedQuery = await searchApi.generateSearchQuery(query);

      // Perform search based on selected mode
      const results =
        searchMode === "semantic"
          ? await searchApi.semanticSearch(enhancedQuery, filters)
          : await searchApi.hybridSearch(enhancedQuery, filters);

      setResults(results);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container mx-auto p-4">
      <div className="space-y-4">
        {/* Search Controls */}
        <Card>
          <CardHeader>
            <CardTitle>Email Search</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex gap-4 mb-4">
              <div className="flex-1">
                <Input
                  type="text"
                  placeholder="Search emails..."
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyPress={(e) => e.key === "Enter" && handleSearch()}
                />
              </div>
              <Select
                value={searchMode}
                onValueChange={(value: "semantic" | "hybrid") =>
                  setSearchMode(value)
                }
              >
                <SelectTrigger className="w-[180px]">
                  <SelectValue placeholder="Search Mode" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="semantic">Semantic Search</SelectItem>
                  <SelectItem value="hybrid">Hybrid Search</SelectItem>
                </SelectContent>
              </Select>
              <Button onClick={handleSearch} disabled={loading}>
                {loading ? "Searching..." : "Search"}
              </Button>
            </div>

            {/* Filters */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label>Date Range</Label>
                <div className="flex gap-2">
                  <Input
                    type="date"
                    onChange={(e) =>
                      setFilters((prev) => ({
                        ...prev,
                        dateRange: {
                          ...prev.dateRange,
                          start: e.target.value,
                        },
                      }))
                    }
                  />
                  <Input
                    type="date"
                    onChange={(e) =>
                      setFilters((prev) => ({
                        ...prev,
                        dateRange: {
                          ...prev.dateRange,
                          end: e.target.value,
                        },
                      }))
                    }
                  />
                </div>
              </div>
              <div>
                <Label>Sender</Label>
                <Input
                  type="text"
                  placeholder="Filter by sender..."
                  onChange={(e) =>
                    setFilters((prev) => ({
                      ...prev,
                      sender: e.target.value,
                    }))
                  }
                />
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Error Display */}
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {/* Results */}
        <div className="space-y-4">
          {results.map((email) => (
            <Card key={email.message_id}>
              <CardHeader>
                <CardTitle>{email.subject}</CardTitle>
                <div className="text-sm text-gray-500">
                  From: {email.sender} | To: {email.recipients}
                </div>
                <div className="text-sm text-gray-500">
                  Date: {new Date(email.date_val).toLocaleDateString()}
                </div>
              </CardHeader>
              <CardContent>
                <p className="mb-2">{email.body}</p>
                {email.labels.length > 0 && (
                  <div className="flex gap-2 mt-2">
                    {email.labels.map((label) => (
                      <span
                        key={label}
                        className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full text-sm"
                      >
                        {label}
                      </span>
                    ))}
                  </div>
                )}
                {email._additional && (
                  <div className="text-sm text-gray-500 mt-2">
                    {email._additional.distance !== undefined &&
                      `Similarity: ${(1 - email._additional.distance).toFixed(
                        3
                      )}`}
                    {email._additional.score !== undefined &&
                      `Score: ${email._additional.score.toFixed(3)}`}
                  </div>
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
};

export default EmailSearch;
