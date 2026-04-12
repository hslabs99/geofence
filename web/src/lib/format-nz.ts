/**
 * Thousands separators for display (NZ English: comma). Use for integer counts and whole-minute values.
 */
export function formatIntNz(n: number): string {
  return Math.round(n).toLocaleString('en-NZ', { maximumFractionDigits: 0, useGrouping: true });
}
