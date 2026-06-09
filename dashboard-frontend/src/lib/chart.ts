import uPlot from 'uplot';
import { formatNumber, formatTime, niceTicks, robustMax } from './format';

// uPlot configuration that reproduces the previous hand-rolled SVG chart:
// blue 2px line over a vertical blue-fade area fill, nice Y ticks scaled to p98,
// HH:MM:SS X labels, and a dark value/time tooltip following the cursor.

const ACCENT = '#2563eb';

function areaFill(u: uPlot): CanvasGradient | string {
  const ctx = u.ctx;
  const { top, height } = u.bbox;
  const g = ctx.createLinearGradient(0, top, 0, top + height);
  g.addColorStop(0, 'rgba(37,99,235,0.22)');
  g.addColorStop(1, 'rgba(37,99,235,0)');
  return g;
}

export interface ChartHandle {
  setData(times: number[], values: number[], unit: string): void;
  destroy(): void;
}

function tooltipPlugin(unit: () => string): uPlot.Plugin {
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
        const u_ = unit();
        const left = u.valToPos(t, 'x');
        const top = u.valToPos(v, 'y');
        tip.style.display = 'block';
        tip.style.left = left + 'px';
        tip.style.top = top + 'px';
        tip.innerHTML =
          `<span class="tip-v">${formatNumber(v)}${u_ ? ' ' + u_ : ''}</span>` +
          `<span class="tip-t">${formatTime(t)}</span>`;
      }
    }
  };
}

export function createChart(el: HTMLElement): ChartHandle {
  let chart: uPlot | null = null;
  let unit = '';
  const unitRef = () => unit;

  function build(width: number, height: number, times: number[], values: number[]) {
    const max = robustMax(values);
    const { ticks, niceMax } = niceTicks(max, 5);

    const opts: uPlot.Options = {
      width,
      height,
      padding: [18, 18, 6, 6],
      cursor: {
        x: true,
        y: false,
        points: { size: 8, width: 2, stroke: ACCENT, fill: '#fff' }
      },
      legend: { show: false },
      scales: {
        x: { time: false },
        y: { range: [0, niceMax] }
      },
      axes: [
        {
          stroke: '#6b7280',
          grid: { show: false },
          ticks: { show: true, stroke: '#d1d5db', size: 4 },
          font: '15px inherit',
          values: (_u, vals) => vals.map((v) => formatTime(v))
        },
        {
          stroke: '#6b7280',
          grid: { stroke: '#eef0f3', width: 1 },
          ticks: { show: false },
          font: '15px inherit',
          splits: () => ticks,
          values: (_u, vals) => vals.map((v) => formatNumber(v))
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
      plugins: [tooltipPlugin(unitRef)]
    };
    chart = new uPlot(opts, [times, values], el);
  }

  return {
    setData(times, values, u) {
      unit = u || '';
      const rect = el.getBoundingClientRect();
      const width = Math.max(1, Math.floor(rect.width));
      const height = Math.max(1, Math.floor(rect.height));
      if (!chart) {
        build(width, height, times, values);
        return;
      }
      // Y range depends on the data, so rebuild splits/range by recreating.
      chart.destroy();
      chart = null;
      build(width, height, times, values);
    },
    destroy() {
      if (chart) {
        chart.destroy();
        chart = null;
      }
    }
  };
}
