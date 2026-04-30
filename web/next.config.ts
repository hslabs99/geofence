import type { NextConfig } from "next";
import path from "node:path";
import { fileURLToPath } from "node:url";

/** This file lives in `web/`; pin Turbopack root so the parent `geodata/package-lock.json` is not treated as the app root. */
const webRoot = path.dirname(fileURLToPath(import.meta.url));

const nextConfig: NextConfig = {
  output: "standalone",
  serverExternalPackages: ["pg"],
  turbopack: {
    root: webRoot,
  },
};

export default nextConfig;
