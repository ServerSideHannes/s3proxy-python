import uPlot from 'uplot';
import { formatNumber, formatTime, niceTicks } from './format';

// uPlot configuration that reproduces the previous hand-rolled SVG chart:
// blue 2px line over a vertical blue-fade area fill, nice Y ticks, HH:MM:SS X
// labels, and a dark value/time tooltip following the cursor.

const ACCENT = '#2563eb';
const AXIS_FONT = '13px -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Helvetica, Arial, sans-serif';
const AXIS_COLOR = '#6b7280';
const GRID_COLOR = '#eef0f3';

function areaFill(u: uPlot): CanvasGradient | string {
  const ctx = u.ctx;
  const { top, height } = u.bbox;
  const g = ctx.createLinearGradient(0, top, 0, top + height);
  g.addColorStop(0, 'rgba(37,99,235,0.22)');
  g.addColorStop(1, 'rgba(37,99,235,0)');
  return g;
}

// A value formatter for the Y axis + tooltip. Returns a fully-formatted string
// INCLUDING any unit (e.g. "2.5 GB" or "1.3k req"), so the chart never appends
// a unit itself — that's what produced "1.3B GB".
export type ValueFormat = (v: number) => string;

export interface ChartHandle {
  setData(times: number[], values: number[], format: ValueFormat): void;
  resize(): void;
  destroy(): void;
}

function tooltipPlugin(fmt: () => ValueFormat): uPlot.Plugin {
  let tip: HTMLDivElement;
  return {
    hooks: {
      init: (u) => {
        tip = document.createElement('div');
        tip.className = 'u-tooltip';
        u.over.appendChild(tip);
        u.over.addEventListener('mouseleave', () => (tip.style.display = 'none'));
      },
      setCursor: (u) => {
        const idx = u.cursor.idx;
        if (idx == null) {
          tip.style.display = 'none';
          return;
        }
        const t = u.data[0][idx];
        const v = u.data[1][idx];
        if (t == null || v == null) {
          tip.style.display = 'none';
          return;
        }
        const left = u.valToPos(t, 'x');
        const top = u.valToPos(v, 'y');
        tip.style.display = 'block';
        tip.style.left = left + 'px';
        tip.style.top = top + 'px';
        tip.innerHTML =
          `<span class="tip-v">${fmt()(v)}</span>` +
          `<span class="tip-t">${formatTime(t)}</span>`;
      }
    }
  };
}

export function createChart(el: HTMLElement): ChartHandle {
  let chart: uPlot | null = null;
  let format: ValueFormat = formatNumber;
  // Y top is data-dependent; uPlot reads it through the scale range callback so
  // we can update it on setData without recreating the chart.
  let niceMax = 1;
  let ticks: number[] = [0, 1];

  function recomputeY(values: number[]) {
    // Scale to the TRUE max (rounded up to a nice value) so the whole line is
    // visible with headroom and the top gridline is a real, labelled tick.
    const max = Math.max(...values, 1);
    const r = niceTicks(max, 5);
    niceMax = r.niceMax;
    ticks = r.ticks;
  }

  function measure(): { width: number; height: number } {
    const rect = el.getBoundingClientRect();
    return {
      width: Math.max(1, Math.floor(rect.width)),
      height: Math.max(1, Math.floor(rect.height))
    };
  }

  function build(times: number[], values: number[]) {
    recomputeY(values);
    const { width, height } = measure();

    const opts: uPlot.Options = {
      width,
      height,
      padding: [18, 20, 6, 12],
      cursor: {
        x: true,
        y: false,
        points: { size: 8, width: 2, stroke: ACCENT, fill: '#fff' }
      },
      legend: { show: false },
      scales: {
        x: { time: false },
        // Read niceMax lazily so range tab switches update the top without a rebuild.
        y: { range: () => [0, niceMax] }
      },
      axes: [
        {
          stroke: AXIS_COLOR,
          grid: { show: false },
          ticks: { show: true, stroke: '#d1d5db', size: 4 },
          font: AXIS_FONT,
          size: 34,
          values: (_u, vals) => vals.map((v) => formatTime(v))
        },
        {
          stroke: AXIS_COLOR,
          grid: { stroke: GRID_COLOR, width: 1 },
          ticks: { show: false },
          font: AXIS_FONT,
          // Reserve enough left gutter that multi-digit/byte labels aren't clipped.
          size: 64,
          splits: () => ticks,
          values: (_u, vals) => vals.map((v) => format(v))
        }
      ],
      series: [
        {},
        {
          stroke: ACCENT,
          width: 2,
          fill: areaFill,
          points: { show: false }
        }
      ],
      plugins: [tooltipPlugin(() => format)]
    };
    chart = new uPlot(opts, [times, values], el);
  }

  return {
    setData(times, values, fmt) {
      format = fmt;
      if (!chart) {
        build(times, values);
        return;
      }
      // Update in place — never destroy/recreate. Recreating re-measures the
      // container (which has aspect-ratio sizing) and made the chart grow on
      // every range click; setData + an explicit size keep it stable.
      recomputeY(values);
      const { width, height } = measure();
      chart.setSize({ width, height });
      chart.setData([times, values]);
    },
    // Reflow to the current container size (e.g. window resize) without changing data.
    resize() {
      if (chart) chart.setSize(measure());
    },
    destroy() {
      if (chart) {
        chart.destroy();
        chart = null;
      }
    }
  };
}
