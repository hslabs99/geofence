import type { Viewport } from 'next';
import LocationServiceStatus from '@/components/LocationServiceStatus';
import { DriverRouteNav } from '@/components/driver/driver-nav';

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  viewportFit: 'cover',
};

/** Phone frame for /driver only. Staff pages do not use this layout. */
export default function DriverLayout({ children }: { children: React.ReactNode }) {
  return (
    <DriverRouteNav>
      <div className="min-h-dvh bg-[#d5dee6] text-[#1c2430]">
        <div className="relative mx-auto flex min-h-dvh w-full max-w-[430px] flex-col bg-[#f4f7f8] shadow-[0_0_0_1px_rgba(15,23,42,0.06)]">
          <LocationServiceStatus variant="phone" />
          {children}
        </div>
      </div>
    </DriverRouteNav>
  );
}
