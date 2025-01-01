interface Email {
    message_id: string;
    subject: string;
    sender: string;
    recipients: string;
    date_val: string;
    thread_id: string;
    labels: string[];
    body: string;
    entities: Array<{
      text: string;
      label: string;
    }>;
    keywords: string[];
    _additional?: {
      distance?: number;
      score?: number;
    };
  }
  
  interface SearchFilters {
    dateRange?: {
      start: string;
      end: string;
    };
    sender?: string;
    labels?: string[];
    entities?: Array<{
      text: string;
      label: string;
    }>;
  }