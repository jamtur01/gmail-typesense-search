// src/types/email.ts

// Base entity interface
interface Entity {
    text: string;
    label: string;
    start: number;
    end: number;
    description: string;
    context: string;
    importance: number;
  }
  
  // Keyword interface
  interface Keyword {
    word: string;
    score: number;
  }
  
  // Main email interface
  interface Email {
    message_id: string;
    subject: string;
    sender: string;
    recipients: string;
    date_val: string;  // ISO format date string
    thread_id: string;
    labels: string[];
    body: string;
    entities: Entity[];
    keywords: Keyword[];
    _additional?: {
      certainty: number;
    };
  }
  
  // Search filters interface
  interface EmailFilters {
    dateRange: {
      start: string | null;
      end: string | null;
    };
    sender: string;
    recipients: string;
    labels: string[];
    entityTypes: string[];
    entityImportance: [number, number];  // [min, max]
    keywordScore: [number, number];      // [min, max]
    threadId: string;
    sortBy: 'date_val' | '_additional.certainty' | 'sender';
    sortDirection: 'asc' | 'desc';
  }
  
  // API response interfaces
  interface WeaviateResponse {
    data: {
      Get: {
        Email: Email[];
      };
    };
    errors?: Array<{
      message: string;
    }>;
  }
  
  interface WeaviateAggregateResponse {
    data: {
      Aggregate: {
        Email: {
          labels: {
            uniqueValues: string[];
          };
          sender: {
            uniqueValues: string[];
          };
          entities: {
            label: {
              uniqueValues: string[];
            };
          };
        };
      };
    };
  }
  
  export type {
    Entity,
    Keyword,
    Email,
    EmailFilters,
    WeaviateResponse,
    WeaviateAggregateResponse
  };