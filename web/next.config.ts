import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  serverExternalPackages: ["@prisma/client", "prisma", "pg"],
  // Standalone: include Prisma so runtime can resolve @prisma/client (fixes "Cannot find module @prisma/client-...")
  outputFileTracingIncludes: {
    "/api/**": ["./node_modules/.prisma/**", "./node_modules/@prisma/client/**"],
    "/api/health/db": ["./node_modules/.prisma/**", "./node_modules/@prisma/client/**"],
    "/api/vworkjobs": ["./node_modules/.prisma/**", "./node_modules/@prisma/client/**"],
    "/api/admin/devices/sync-from-vworkjobs": ["./node_modules/.prisma/**", "./node_modules/@prisma/client/**"],
  },
};

export default nextConfig;
