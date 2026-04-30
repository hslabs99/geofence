'use client';

import { useParams } from 'next/navigation';
import { useEffect, useState } from 'react';

/**
 * Redirects to Google Maps at a point inside the geofence (PostGIS ST_PointOnSurface).
 * KML-by-URL in Maps is unreliable for app-hosted endpoints; lat/lon matches Inspect “View on map”.
 */
export default function GeofenceMapRedirectPage() {
  const params = useParams();
  const id = typeof params?.id === 'string' ? params.id : null;
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id || typeof window === 'undefined') return;
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`/api/admin/geofences/${id}`);
        const data = await res.json().catch(() => ({}));
        if (cancelled) return;
        if (!res.ok) {
          setError(typeof data?.error === 'string' ? data.error : `Failed (${res.status})`);
          return;
        }
        const lat = typeof data.center_lat === 'number' ? data.center_lat : NaN;
        const lon = typeof data.center_lon === 'number' ? data.center_lon : NaN;
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          setError('No map coordinates for this geofence');
          return;
        }
        window.location.href = `https://www.google.com/maps?q=${lat},${lon}`;
      } catch {
        if (!cancelled) setError('Could not load geofence');
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [id]);

  return (
    <div className="flex min-h-[200px] items-center justify-center p-6">
      <p className="text-sm text-zinc-500 dark:text-zinc-400">
        {error ?? 'Opening location in Google Maps…'}
      </p>
    </div>
  );
}
