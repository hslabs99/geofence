'use client';

import { useState } from 'react';
import Link from 'next/link';
import { GEODATA_USER_STORAGE_KEY } from '@/contexts/ViewModeContext';

export default function DriverLoginPage() {
  const [driverId, setDriverId] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const res = await fetch('/api/auth/driver-login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ driverId: driverId.trim(), password }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error ?? 'Login failed');
        return;
      }
      const truckRego = data.user?.truckRego ?? null;
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(
          GEODATA_USER_STORAGE_KEY,
          JSON.stringify({
            email: data.user?.email,
            userType: 'Driver',
            driverName: data.user?.driverName ?? data.user?.firstname ?? null,
            ...(truckRego != null && { truckRego }),
          }),
        );
      }
      window.location.href = '/driver/home';
      return;
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Network error');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-1 flex-col px-5 pb-[max(1.5rem,env(safe-area-inset-bottom))] pt-[max(2.5rem,env(safe-area-inset-top))]">
      <p className="text-sm font-semibold uppercase tracking-[0.18em] text-[#1f9d55]">Driver</p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight text-[#1c2430]">Sign in</h1>
      <p className="mt-2 text-base leading-snug text-[#667085]">Use your driver ID. This screen is for the phone.</p>

      <form onSubmit={handleSubmit} className="mt-8 flex flex-1 flex-col gap-4">
        <div>
          <label htmlFor="driver-id" className="mb-1.5 block text-sm font-medium text-[#344054]">
            Driver ID
          </label>
          <input
            id="driver-id"
            type="text"
            autoComplete="username"
            autoCapitalize="none"
            required
            value={driverId}
            onChange={(e) => setDriverId(e.target.value)}
            placeholder="Mike"
            className="h-14 w-full rounded-2xl border border-[#d0d5dd] bg-white px-4 text-lg text-[#1c2430] outline-none focus:border-[#1f9d55]"
          />
        </div>
        <div>
          <label htmlFor="driver-password" className="mb-1.5 block text-sm font-medium text-[#344054]">
            Password
          </label>
          <input
            id="driver-password"
            type="password"
            autoComplete="current-password"
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="h-14 w-full rounded-2xl border border-[#d0d5dd] bg-white px-4 text-lg text-[#1c2430] outline-none focus:border-[#1f9d55]"
          />
        </div>
        {error && <p className="text-base text-[#b42318]">{error}</p>}
        <button
          type="submit"
          disabled={loading}
          className="mt-2 h-14 w-full rounded-2xl bg-[#1f9d55] text-lg font-semibold text-white disabled:opacity-60"
        >
          {loading ? 'Signing in…' : 'Sign in'}
        </button>
        <p className="mt-auto pt-8 text-center text-sm text-[#667085]">
          <Link href="/login" className="underline">
            Staff sign in
          </Link>
        </p>
      </form>
    </div>
  );
}
