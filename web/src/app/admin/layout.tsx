'use client';

import StaffOnlyGate from '@/components/StaffOnlyGate';

/** All /admin/* routes: Client users are redirected to Summary. */
export default function AdminLayout({ children }: { children: React.ReactNode }) {
  return <StaffOnlyGate>{children}</StaffOnlyGate>;
}
