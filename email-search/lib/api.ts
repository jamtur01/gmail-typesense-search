"use client";

import weaviate from "weaviate-client";

export class EmailSearchApi {
  private client: any;
  private openaiKey: string;

  constructor(weaviateUrl: string, openaiKey: string) {
    const [host, port] = weaviateUrl.split(":");

    this.client = weaviate.connectToCustom({
      httpHost: host,
      httpPort: parseInt(port) || 8080,
      httpSecure: false, // set to true if using HTTPS
      authCredentials: new weaviate.ApiKey(process.env.WEAVIATE_API_KEY || ""),
      headers: {
        "X-OpenAI-API-Key": openaiKey,
      },
    });

    this.openaiKey = openaiKey;
  }

  async semanticSearch(
    query: string,
    filters?: SearchFilters,
    limit: number = 10
  ): Promise<Email[]> {
    try {
      let whereFilter: any = {};

      if (filters?.dateRange) {
        whereFilter.path = ["date_val"];
        whereFilter.operator = "And";
        whereFilter.operands = [
          {
            path: ["date_val"],
            operator: "GreaterThanEqual",
            valueDate: filters.dateRange.start,
          },
          {
            path: ["date_val"],
            operator: "LessThanEqual",
            valueDate: filters.dateRange.end,
          },
        ];
      }

      const response = await this.client.graphql
        .get()
        .withClassName("Email")
        .withFields(
          "message_id subject sender recipients date_val thread_id labels body entities { text label } keywords _additional { distance }"
        )
        .withNearText({
          concepts: [query],
          certainty: 0.7,
        })
        .withWhere(whereFilter)
        .withLimit(limit)
        .do();

      return response.data.Get.Email;
    } catch (error) {
      console.error("Semantic search error:", error);
      throw error;
    }
  }

  async hybridSearch(
    query: string,
    filters?: SearchFilters,
    limit: number = 10
  ): Promise<Email[]> {
    try {
      let whereFilter: any = {};

      // Apply filters similar to semantic search
      if (filters) {
        // ... same filter logic as above
      }

      const response = await this.client.graphql
        .get()
        .withClassName("Email")
        .withFields(
          "message_id subject sender recipients date_val thread_id labels body entities { text label } keywords _additional { score }"
        )
        .withHybrid({
          query,
          alpha: 0.5, // Blend between vector and keyword search
          properties: ["subject^2", "body"], // Boost subject relevance
        })
        .withWhere(whereFilter)
        .withLimit(limit)
        .do();

      return response.data.Get.Email as Email[];
    } catch (error) {
      console.error("Hybrid search error:", error);
      throw error;
    }
  }

  async generateSearchQuery(userInput: string): Promise<string> {
    try {
      const response = await fetch("https://api.openai.com/v1/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.openaiKey}`,
        },
        body: JSON.stringify({
          model: "gpt-3.5-turbo-instruct",
          prompt: `Convert this email search request into a clear search query: "${userInput}"`,
          max_tokens: 50,
          temperature: 0.3,
        }),
      });

      const data = await response.json();
      return data.choices[0].text.trim();
    } catch (error) {
      console.error("Query generation error:", error);
      return userInput;
    }
  }
}
