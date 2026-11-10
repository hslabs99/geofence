'use client';

import { JobQueueIssues } from '@/components/driver/JobQueueIssues';

/** Public page from the mapping email. No login. The first save resolves the mapping for later visitors. */
export default function ResolveMappingsPage() {
  return (
    <div className="mx-auto max-w-5xl px-4 py-6">
      <h1 className="mb-4 text-xl font-semibold text-zinc-900 dark:text-zinc-100">Job Queue Issues</h1>
      <JobQueueIssues />
    </div>
  );
}
