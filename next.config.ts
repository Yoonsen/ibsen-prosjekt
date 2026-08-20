import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    if (process.env.NODE_ENV === "development") {
      return [
        {
          source: "/api/:path*",
          destination: "http://127.0.0.1:8000/api/:path*", 
        },
      ];
    }
    return [
        {
          source: "/api/analyze-source",
          destination: "/api/analyze",
        },
        {
          source: "/api/source-works",
          destination: "/api/analyze",
        }
    ];
  },
};

export default nextConfig;
