// Number/time formatting ported verbatim from the inline dashboard JS so chart
// axes, tooltips, and breakdown bars match the previous renderer exactly.

export function formatNumber(v: number): string {
  if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'k';
  if (Math.abs(v) < 1 && v !== 0) return v.toFixed(2);
  return Math.round(v).toString();
}

export function niceCeil(x: number): number {
  if (x <= 0) return 1;
  const exp = Math.floor(Math.log10(x));
  const base = Math.pow(10, exp);
  const norm = x / base;
  let step: number;
  if (norm <= 1) step = 1;
  else if (norm <= 2) step = 2;
  else if (norm <= 2.5) step = 2.5;
  else if (norm <= 5) step = 5;
  else step = 10;
  return step * base;
}

export function niceTicks(maxV: number, target = 5): { ticks: number[]; niceMax: number } {
  const niceMax = niceCeil(maxV);
  const rawStep = niceMax / target;
  const exp = Math.floor(Math.log10(rawStep || 1));
  const base = Math.pow(10, exp);
  const norm = rawStep / base;
  let step: number;
  if (norm <= 1) step = 1 * base;
  else if (norm <= 2) step = 2 * base;
  else if (norm <= 2.5) step = 2.5 * base;
  else if (norm <= 5) step = 5 * base;
  else step = 10 * base;
  const ticks: number[] = [];
  for (let v = 0; v <= niceMax + 1e-9; v += step) ticks.push(v);
  return { ticks, niceMax };
}

export function formatTime(epochSeconds: number): string {
  const d = new Date(epochSeconds * 1000);
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

export function formatIsoShort(iso?: string): string {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch {
    return iso;
  }
}

// Robust Y max: scale to p98 so a single early spike doesn't squash the series
// flat. Matches the previous chart's behaviour.
export function robustMax(vals: number[]): number {
  const sorted = vals.slice().sort((a, b) => a - b);
  const p98 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.98))] || 0;
  const rawMax = Math.max(...vals, 1);
  return Math.max(1, p98 > 0 ? p98 : rawMax);
}
