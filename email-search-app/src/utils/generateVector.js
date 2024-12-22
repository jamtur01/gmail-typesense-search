// src/utils/generateVector.js
import axios from 'axios';

export const generateVector = async (query) => {
  try {
    const response = await axios.post('https://api.openai.com/v1/embeddings', {
      input: query,
      model: 'text-embedding-ada-002', // Example model
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${import.meta.env.VITE_OPENAI_API_KEY}`,
      },
    });

    return response.data.data[0].embedding; // Adjust based on API response
  } catch (error) {
    console.error('Error generating vector:', error);
    // Return a default vector or handle the error as needed
    return Array(1536).fill(0.1);
  }
};

