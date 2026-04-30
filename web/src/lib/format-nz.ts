/**
 * Thousands separators for display (NZ English: comma). Use for integer counts and whole-minute values.
 */
export function formatIntNz(n: number): string {
  return Math.round(n).toLocaleString('en-NZ', { maximumFractionDigits: 0, useGrouping: true });
}

/** Kilometres with thousands separators and exactly two decimal places (en-NZ). */
export function formatKmNz2(n: number): string {
  return n.toLocaleString('en-NZ', { minimumFractionDigits: 2, maximumFractionDigits: 2, useGrouping: true });
}
