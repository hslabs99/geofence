import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  serverExternalPackages: ["@prisma/client", "prisma"],
  // Standalone: include Prisma so runtime can resolve @prisma/client (fixes "Cannot find module @prisma/client-...")
  outputFileTracingIncludes: {
    "/api/**": ["./node_modules/.prisma/**", "./node_modules/@prisma/client/**"],
  },
};

export default nextConfig;
