'use client';

import { useRequireStaffUser } from '@/hooks/useRequireStaffUser';

/** Blocks staff-only pages until session is known; redirects Client users to Summary. */
export default function StaffOnlyGate({ children }: { children: React.ReactNode }) {
  const { blocked } = useRequireStaffUser();
  if (blocked) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center p-6 text-sm text-zinc-500 dark:text-zinc-400">
        Loading…
      </div>
    );
  }
  return <>{children}</>;
}
