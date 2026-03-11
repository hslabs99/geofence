'use client';

import { useParams } from 'next/navigation';
import { useEffect } from 'react';

/**
 * Redirects to Google Maps with the geofence KML so the outline is shown.
 * We don't render the map in the app; Google Maps shows the polygon.
 */
export default function GeofenceMapRedirectPage() {
  const params = useParams();
  const id = typeof params?.id === 'string' ? params.id : null;

  useEffect(() => {
    if (!id || typeof window === 'undefined') return;
    const kmlUrl = `${window.location.origin}/api/admin/geofences/${id}/kml`;
    window.location.href = `https://www.google.com/maps?q=${encodeURIComponent(kmlUrl)}`;
  }, [id]);

  return (
    <div className="flex min-h-[200px] items-center justify-center p-6">
      <p className="text-sm text-zinc-500 dark:text-zinc-400">
        Opening geofence outline in Google Maps…
      </p>
    </div>
  );
}
