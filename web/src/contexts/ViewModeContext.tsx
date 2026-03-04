'use client';

import React, { createContext, useContext, useState } from 'react';

/** Super and Admin both use admin view in Summary; Client uses client view. */
export type ViewMode = 'super' | 'admin' | 'client';

const ViewModeContext = createContext<{
  viewMode: ViewMode;
  setViewMode: (mode: ViewMode) => void;
  /** When viewMode === 'client', Summary (and similar) lock customer filter to this value. */
  clientCustomer: string;
  setClientCustomer: (value: string) => void;
} | null>(null);

export function ViewModeProvider({ children }: { children: React.ReactNode }) {
  const [viewMode, setViewMode] = useState<ViewMode>('admin');
  const [clientCustomer, setClientCustomer] = useState('');
  return (
    <ViewModeContext.Provider value={{ viewMode, setViewMode, clientCustomer, setClientCustomer }}>
      {children}
    </ViewModeContext.Provider>
  );
}

/** True when Summary (and similar) should show client view (reduced columns). */
export function isClientView(mode: ViewMode): boolean {
  return mode === 'client';
}

export function useViewMode() {
  const ctx = useContext(ViewModeContext);
  if (!ctx) throw new Error('useViewMode must be used within ViewModeProvider');
  return ctx;
}
