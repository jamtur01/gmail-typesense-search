import React, { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Slider } from "@/components/ui/slider";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Mail,
  Tag,
  User,
  Calendar,
  Hash,
  Filter,
  SlidersHorizontal,
  Loader2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { Email, EmailFilters } from "../types/email";

// Event and prop types
type SearchEvent = React.ChangeEvent<HTMLInputElement>;
type SelectEvent = string;
type CheckboxEvent = boolean;
type DateEvent = React.ChangeEvent<HTMLInputElement>;
type SliderEvent = number[];

interface EmailSearchProps {
  className?: string;
}

interface AvailableFilters {
  labels: string[];
  entityTypes: string[];
  senders: string[];
}

const ITEMS_PER_PAGE = 10;
const WEAVIATE_ENDPOINT = import.meta.env.VITE_WEAVIATE_ENDPOINT;

export const EmailSearch: React.FC<EmailSearchProps> = ({ className }) => {
  const [query, setQuery] = useState<string>("");
  const [results, setResults] = useState<Email[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState<number>(1);
  const [totalResults, setTotalResults] = useState<number>(0);

  const [availableFilters, setAvailableFilters] = useState<AvailableFilters>({
    labels: [],
    entityTypes: [],
    senders: [],
  });

  const [advancedFilters, setAdvancedFilters] = useState<EmailFilters>({
    dateRange: {
      start: null,
      end: null,
    },
    sender: "",
    recipients: "",
    labels: [],
    entityTypes: [],
    entityImportance: [0, 10],
    keywordScore: [0, 1],
    threadId: "",
    sortBy: "date_val",
    sortDirection: "desc",
  });

  // Fetch available filter options
  useEffect(() => {
    const fetchFilterOptions = async () => {
      try {
        const response = await fetch(`${WEAVIATE_ENDPOINT}/graphql`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            query: `
              {
                Get {
                  Email(limit: 0) {
                    labels
                    sender
                    entities {
                      label
                    }
                  }
                }
              }
            `,
          }),
        });

        const data = await response.json();

        if (data.data?.Get?.Email) {
          // Extract unique values from the data
          const extractUniqueValues = (items: any[], path: string) => {
            const uniqueSet = new Set(
              items
                .map((item) =>
                  path.split(".").reduce((obj, key) => obj?.[key], item)
                )
                .filter(Boolean)
            );
            return Array.from(uniqueSet);
          };

          setAvailableFilters({
            labels: extractUniqueValues(data.data.Get.Email, "labels"),
            entityTypes: extractUniqueValues(
              data.data.Get.Email,
              "entities.label"
            ),
            senders: extractUniqueValues(data.data.Get.Email, "sender"),
          });
        }
      } catch (error) {
        console.error("Error fetching filter options:", error);
      }
    };

    fetchFilterOptions();
  }, []);

  const searchEmails = async (
    searchQuery: string,
    filters: EmailFilters,
    currentPage: number
  ): Promise<void> => {
    try {
      setLoading(true);
      setError(null);

      // Construct where filter
      const whereFilter: any[] = [];
      // ... [rest of your filter construction logic]

      const offset = (currentPage - 1) * ITEMS_PER_PAGE;

      // Helper function to construct the where clause
      const constructWhereClause = () => {
        if (whereFilter.length === 0) return "";
        return `where: { 
                  operator: And, 
                  operands: ${JSON.stringify(whereFilter)}
                }`;
      };

      const query = `
        {
          Get {
            EmailResults: Email(
              ${constructWhereClause()}
              limit: ${ITEMS_PER_PAGE}
              offset: ${offset}
              sort: [{ path: "${filters.sortBy}", order: ${
        filters.sortDirection === "desc" ? "desc" : "asc"
      } }]
            ) {
              message_id
              subject
              sender
              recipients
              date_val
              thread_id
              labels
              body
              entities {
                text
                label
                description
                importance
              }
              keywords {
                word
                score
              }
              _additional {
                certainty
              }
            }
          }
          Aggregate {
            Email ${
              constructWhereClause() ? `(${constructWhereClause()})` : ""
            } {
              meta {
                count
              }
            }
          }
        }
      `;

      const response = await fetch(`${WEAVIATE_ENDPOINT}/graphql`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query }),
      });

      const data = await response.json();

      if (data.errors) {
        throw new Error(data.errors[0].message);
      }

      setResults(data.data.Get.EmailResults || []);
      setTotalResults(data.data.Aggregate.Email?.[0]?.meta.count || 0);
    } catch (error) {
      if (error instanceof Error) {
        setError(error.message);
      } else {
        setError("An unknown error occurred");
      }
      setResults([]);
      setTotalResults(0);
    } finally {
      setLoading(false);
    }
  };

  // Debounce search
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      if (query || Object.values(advancedFilters).some((v) => v)) {
        searchEmails(query, advancedFilters, page);
      }
    }, 300);

    return () => clearTimeout(timeoutId);
  }, [query, advancedFilters, page]);

  const formatDate = (dateString: string): string => {
    return new Date(dateString).toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const handleFilterChange = (
    value: SelectEvent | CheckboxEvent,
    filterType: keyof EmailFilters
  ): void => {
    setAdvancedFilters((prev) => ({
      ...prev,
      [filterType]: value,
    }));
  };

  const totalPages = Math.ceil(totalResults / ITEMS_PER_PAGE);

  return (
    <div className={cn("max-w-6xl mx-auto p-4", className)}>
      <div className="mb-8">
        {/* Search Bar and Basic Filters */}
        <div className="flex gap-4 mb-4">
          <Input
            type="search"
            placeholder="Search emails..."
            value={query}
            onChange={(e: SearchEvent) => setQuery(e.target.value)}
            className="flex-1"
          />

          <Popover>
            <PopoverTrigger asChild>
              <Button variant="outline">
                <Filter className="w-4 h-4 mr-2" />
                Filters
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-80">
              <Accordion type="single" collapsible>
                {/* Date Range */}
                <AccordionItem value="date">
                  <AccordionTrigger>
                    <Calendar className="w-4 h-4 mr-2" />
                    Date Range
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-2">
                      <Input
                        type="date"
                        onChange={(e: DateEvent) => {
                          setAdvancedFilters((prev) => ({
                            ...prev,
                            dateRange: {
                              ...prev.dateRange,
                              start: e.target.value,
                            },
                          }));
                        }}
                      />
                      <Input
                        type="date"
                        onChange={(e: DateEvent) => {
                          setAdvancedFilters((prev) => ({
                            ...prev,
                            dateRange: {
                              ...prev.dateRange,
                              end: e.target.value,
                            },
                          }));
                        }}
                      />
                    </div>
                  </AccordionContent>
                </AccordionItem>

                {/* People */}
                <AccordionItem value="people">
                  <AccordionTrigger>
                    <User className="w-4 h-4 mr-2" />
                    People
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-2">
                      <Select
                        onValueChange={(value: SelectEvent) =>
                          handleFilterChange(value, "sender")
                        }
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select sender" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableFilters.senders.map((sender) => (
                            <SelectItem key={sender} value={sender}>
                              {sender}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Input
                        placeholder="Recipients contains..."
                        onChange={(e: SearchEvent) =>
                          handleFilterChange(e.target.value, "recipients")
                        }
                      />
                    </div>
                  </AccordionContent>
                </AccordionItem>

                {/* Labels */}
                <AccordionItem value="labels">
                  <AccordionTrigger>
                    <Tag className="w-4 h-4 mr-2" />
                    Labels
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-2">
                      {availableFilters.labels.map((label) => (
                        <div
                          key={label}
                          className="flex items-center space-x-2"
                        >
                          <Checkbox
                            id={label}
                            checked={advancedFilters.labels.includes(label)}
                            onCheckedChange={(checked: CheckboxEvent) => {
                              setAdvancedFilters((prev) => ({
                                ...prev,
                                labels: checked
                                  ? [...prev.labels, label]
                                  : prev.labels.filter((l) => l !== label),
                              }));
                            }}
                          />
                          <label htmlFor={label}>{label}</label>
                        </div>
                      ))}
                    </div>
                  </AccordionContent>
                </AccordionItem>

                {/* Entities */}
                <AccordionItem value="entities">
                  <AccordionTrigger>
                    <Hash className="w-4 h-4 mr-2" />
                    Entities
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-4">
                      {availableFilters.entityTypes.map((type) => (
                        <div key={type} className="flex items-center space-x-2">
                          <Checkbox
                            id={type}
                            checked={advancedFilters.entityTypes.includes(type)}
                            onCheckedChange={(checked: CheckboxEvent) => {
                              setAdvancedFilters((prev) => ({
                                ...prev,
                                entityTypes: checked
                                  ? [...prev.entityTypes, type]
                                  : prev.entityTypes.filter((t) => t !== type),
                              }));
                            }}
                          />
                          <label htmlFor={type}>{type}</label>
                        </div>
                      ))}
                      <div className="pt-2">
                        <label className="text-sm mb-2 block">
                          Entity Importance
                        </label>
                        <Slider
                          defaultValue={[0, 10]}
                          max={10}
                          step={1}
                          onValueChange={(value: SliderEvent) => {
                            setAdvancedFilters((prev) => ({
                              ...prev,
                              entityImportance: value as [number, number],
                            }));
                          }}
                        />
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>

                {/* Advanced */}
                <AccordionItem value="advanced">
                  <AccordionTrigger>
                    <SlidersHorizontal className="w-4 h-4 mr-2" />
                    Advanced
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-4">
                      <Input
                        placeholder="Thread ID"
                        value={advancedFilters.threadId}
                        onChange={(e: SearchEvent) =>
                          handleFilterChange(e.target.value, "threadId")
                        }
                      />
                      <div>
                        <label className="text-sm mb-2 block">
                          Keyword Score Range
                        </label>
                        <Slider
                          defaultValue={[0, 1]}
                          min={0}
                          max={1}
                          step={0.1}
                          onValueChange={(value: SliderEvent) => {
                            setAdvancedFilters((prev) => ({
                              ...prev,
                              keywordScore: value as [number, number],
                            }));
                          }}
                        />
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>

                {/* Sort Options */}
                <AccordionItem value="sorting">
                  <AccordionTrigger>
                    <SlidersHorizontal className="w-4 h-4 mr-2" />
                    Sort Options
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-2">
                      <Select
                        value={advancedFilters.sortBy}
                        onValueChange={(value: SelectEvent) =>
                          handleFilterChange(value, "sortBy")
                        }
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Sort by..." />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="date_val">Date</SelectItem>
                          <SelectItem value="_additional.certainty">
                            Relevance
                          </SelectItem>
                          <SelectItem value="sender">Sender</SelectItem>
                        </SelectContent>
                      </Select>

                      <Select
                        value={advancedFilters.sortDirection}
                        onValueChange={(value: SelectEvent) =>
                          handleFilterChange(value, "sortDirection")
                        }
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Sort direction..." />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="desc">Descending</SelectItem>
                          <SelectItem value="asc">Ascending</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            </PopoverContent>
          </Popover>
        </div>

        {/* Active Filters Display */}
        <div className="flex flex-wrap gap-2">
          {advancedFilters.sender && (
            <Badge variant="secondary" className="flex items-center gap-1">
              <User className="w-3 h-3" />
              From: {advancedFilters.sender}
              <button
                className="ml-1"
                onClick={() => handleFilterChange("", "sender")}
                aria-label="Remove sender filter"
              >
                ×
              </button>
            </Badge>
          )}
          {advancedFilters.labels.map((label) => (
            <Badge
              key={label}
              variant="secondary"
              className="flex items-center gap-1"
            >
              <Tag className="w-3 h-3" />
              {label}
              <button
                className="ml-1"
                onClick={() => {
                  setAdvancedFilters((prev) => ({
                    ...prev,
                    labels: prev.labels.filter((l) => l !== label),
                  }));
                }}
                aria-label={`Remove ${label} label filter`}
              >
                ×
              </button>
            </Badge>
          ))}
          {advancedFilters.entityTypes.map((type) => (
            <Badge
              key={type}
              variant="secondary"
              className="flex items-center gap-1"
            >
              <Hash className="w-3 h-3" />
              {type}
              <button
                className="ml-1"
                onClick={() => {
                  setAdvancedFilters((prev) => ({
                    ...prev,
                    entityTypes: prev.entityTypes.filter((t) => t !== type),
                  }));
                }}
                aria-label={`Remove ${type} entity filter`}
              >
                ×
              </button>
            </Badge>
          ))}
        </div>
      </div>

      {/* Results */}
      <div className="space-y-4">
        {/* Results Count */}
        {!loading && totalResults > 0 && (
          <div className="text-sm text-gray-500">
            Showing {(page - 1) * ITEMS_PER_PAGE + 1} -{" "}
            {Math.min(page * ITEMS_PER_PAGE, totalResults)} of {totalResults}{" "}
            results
          </div>
        )}

        {loading && (
          <div
            className="flex justify-center"
            role="status"
            aria-label="Loading"
          >
            <Loader2 className="w-8 h-8 animate-spin" />
          </div>
        )}

        {error && (
          <div className="text-red-500 text-center" role="alert">
            Error: {error}
          </div>
        )}

        {!loading &&
          results.map((email) => (
            <Card key={email.message_id}>
              <CardContent className="p-4">
                <div className="flex items-start justify-between mb-2">
                  <h2 className="text-xl font-semibold">{email.subject}</h2>
                  <div className="text-sm text-gray-500">
                    {formatDate(email.date_val)}
                  </div>
                </div>

                <div className="flex items-center gap-2 text-sm text-gray-600 mb-2">
                  <User className="w-4 h-4" />
                  <span>{email.sender}</span>
                  <Mail className="w-4 h-4 ml-2" />
                  <span>{email.recipients}</span>
                </div>

                <p className="text-gray-700 mb-3">
                  {email.body.substring(0, 200)}...
                </p>

                {/* Labels */}
                <div className="flex flex-wrap gap-2 mb-3">
                  {email.labels.map((label, idx) => (
                    <Badge
                      key={`${email.message_id}-${label}-${idx}`}
                      variant="secondary"
                      className="cursor-pointer"
                      onClick={() => {
                        if (!advancedFilters.labels.includes(label)) {
                          setAdvancedFilters((prev) => ({
                            ...prev,
                            labels: [...prev.labels, label],
                          }));
                        }
                      }}
                      role="button"
                      aria-label={`Add ${label} to filters`}
                    >
                      <Tag className="w-3 h-3 mr-1" />
                      {label}
                    </Badge>
                  ))}
                </div>

                {/* Entities */}
                <div className="flex flex-wrap gap-2 mb-3">
                  {email.entities.map((entity, idx) => (
                    <Badge
                      key={`${email.message_id}-${entity.text}-${idx}`}
                      variant="outline"
                      className="flex items-center cursor-pointer"
                      onClick={() => {
                        if (
                          !advancedFilters.entityTypes.includes(entity.label)
                        ) {
                          setAdvancedFilters((prev) => ({
                            ...prev,
                            entityTypes: [...prev.entityTypes, entity.label],
                          }));
                        }
                      }}
                      role="button"
                      aria-label={`Add ${entity.label} entity type to filters`}
                    >
                      <Hash className="w-3 h-3 mr-1" />
                      {entity.text} ({entity.label})
                      {entity.importance && (
                        <span className="ml-1 text-xs">
                          ({entity.importance})
                        </span>
                      )}
                    </Badge>
                  ))}
                </div>

                {/* Keywords */}
                <div className="text-sm text-gray-500">
                  Keywords:{" "}
                  {email.keywords
                    .map((k) => `${k.word} (${k.score.toFixed(2)})`)
                    .join(", ")}
                </div>

                <div className="mt-2 text-sm text-gray-500">
                  Relevance:{" "}
                  {(email._additional?.certainty ?? 0 * 100).toFixed(1)}%
                </div>
              </CardContent>
            </Card>
          ))}

        {!loading &&
          results.length === 0 &&
          (query || Object.values(advancedFilters).some((v) => v)) && (
            <div className="text-center text-gray-500">No emails found</div>
          )}

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex justify-center gap-2 mt-6">
            <Button
              variant="outline"
              disabled={page === 1}
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              aria-label="Previous page"
            >
              Previous
            </Button>

            {Array.from({ length: totalPages }, (_, i) => i + 1)
              .filter(
                (p) => p === 1 || p === totalPages || Math.abs(p - page) <= 2
              )
              .map((p, i, arr) => (
                <React.Fragment key={p}>
                  {i > 0 && arr[i - 1] !== p - 1 && (
                    <Button variant="outline" disabled>
                      ...
                    </Button>
                  )}
                  <Button
                    variant={p === page ? "default" : "outline"}
                    onClick={() => setPage(p)}
                    aria-label={`Page ${p}`}
                    aria-current={p === page ? "page" : undefined}
                  >
                    {p}
                  </Button>
                </React.Fragment>
              ))}

            <Button
              variant="outline"
              disabled={page === totalPages}
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              aria-label="Next page"
            >
              Next
            </Button>
          </div>
        )}
      </div>
    </div>
  );
};

export default EmailSearch;
