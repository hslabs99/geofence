'use client';

import { usePathname } from 'next/navigation';
import Sidebar from '@/components/Sidebar';

/** Staff shell. The driver phone app under /driver does not use the sidebar or staff page frame. */
export default function AppChrome({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  if (pathname === '/driver' || pathname.startsWith('/driver/')) {
    return <>{children}</>;
  }

  return (
    <div className="flex min-h-screen bg-zinc-50 dark:bg-zinc-950">
      <Sidebar />
      <main className="flex-1 overflow-auto">{children}</main>
    </div>
  );
}
