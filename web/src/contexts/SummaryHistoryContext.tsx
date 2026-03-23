'use client';

import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { usePathname } from 'next/navigation';
import {
  appendSummaryHistory,
  type SummaryHistoryPayload,
} from '@/lib/summary-history-storage';

/** Set on job ID → Inspect click so the saved history row includes that job for restore. */
export type SummaryInspectJumpMeta = {
  jobId: string;
  truckId: string;
  actualFrom: string;
  actualTo: string;
};

type Ctx = {
  /** Latest Summary state for “leave page” capture. */
  registerSummarySnapshot: (payload: SummaryHistoryPayload | null) => void;
  /** Call before navigating to Inspect via a Summary job link (same-tab). */
  registerPendingInspectForHistory: (meta: SummaryInspectJumpMeta | null) => void;
  /** Bump so sidebar re-reads localStorage. */
  historyVersion: number;
};

const SummaryHistoryContext = createContext<Ctx | null>(null);

export function SummaryHistoryProvider({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const prevPathRef = useRef(pathname);
  const snapshotRef = useRef<SummaryHistoryPayload | null>(null);
  const pendingInspectRef = useRef<SummaryInspectJumpMeta | null>(null);
  const [historyVersion, setHistoryVersion] = useState(0);

  const registerSummarySnapshot = useCallback((payload: SummaryHistoryPayload | null) => {
    snapshotRef.current = payload;
  }, []);

  const registerPendingInspectForHistory = useCallback((meta: SummaryInspectJumpMeta | null) => {
    pendingInspectRef.current = meta;
  }, []);

  useEffect(() => {
    const prev = prevPathRef.current;
    prevPathRef.current = pathname;
    if (prev !== '/query/summary' || pathname === '/query/summary') return;
    const p = snapshotRef.current;
    if (!p) return;
    const jump = pendingInspectRef.current;
    pendingInspectRef.current = null;
    const payload: SummaryHistoryPayload = jump
      ? {
          ...p,
          focusJobId: jump.jobId,
          ...(jump.truckId.trim() ? { focusTruckId: jump.truckId.trim() } : {}),
          ...(jump.actualFrom.trim()
            ? { focusInspectActualFrom: jump.actualFrom.trim() }
            : {}),
          ...(jump.actualTo.trim() ? { focusInspectActualTo: jump.actualTo.trim() } : {}),
        }
      : p;
    if (appendSummaryHistory(payload)) setHistoryVersion((v) => v + 1);
  }, [pathname]);

  return (
    <SummaryHistoryContext.Provider
      value={{ registerSummarySnapshot, registerPendingInspectForHistory, historyVersion }}
    >
      {children}
    </SummaryHistoryContext.Provider>
  );
}

export function useSummaryHistory() {
  const ctx = useContext(SummaryHistoryContext);
  if (!ctx) throw new Error('useSummaryHistory must be used within SummaryHistoryProvider');
  return ctx;
}
