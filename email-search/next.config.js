/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    serverActions: {
      allowedOrigins: ["localhost:3000"],
    }
  },
  env: {
    NEXT_PUBLIC_WEAVIATE_URL: process.env.NEXT_PUBLIC_WEAVIATE_URL,
    NEXT_PUBLIC_OPENAI_API_KEY: process.env.NEXT_PUBLIC_OPENAI_API_KEY,
    WEAVIATE_API_KEY: process.env.WEAVIATE_API_KEY,
  },
};
