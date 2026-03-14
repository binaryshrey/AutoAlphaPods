"use client";

import { useEffect, useRef } from "react";

export default function CandlestickBackground() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf: number;

    const resize = () => {
      const dpr = window.devicePixelRatio || 1;
      canvas.width = window.innerWidth * dpr;
      canvas.height = window.innerHeight * dpr;
      canvas.style.width = `${window.innerWidth}px`;
      canvas.style.height = `${window.innerHeight}px`;
      ctx.scale(dpr, dpr);
    };
    window.addEventListener("resize", resize);
    resize();

    // ── Chart constants ──────────────────────────────────────────────────────
    const CW = 28;           // candle body width
    const GAP = 12;          // gap between candles
    const STEP = CW + GAP;   // total candle slot width
    const SCROLL = 0.6;      // px per frame
    const MA_PERIOD = 20;

    interface Candle {
      open: number;
      close: number;
      high: number;
      low: number;
      volume: number;
    }

    let price = window.innerHeight * 0.5;
    let momentum = 0;

    const maxCandles = Math.ceil(window.innerWidth / STEP) + 12;
    const candles: Candle[] = [];

    // Seed historical candles
    for (let i = 0; i < maxCandles; i++) {
      momentum += (Math.random() - 0.5) * 22;
      momentum *= 0.93;
      const change = momentum + (Math.random() - 0.5) * 70;
      const open = price;
      const close = price - change;
      const high = Math.min(open, close) - Math.random() * 60;
      const low = Math.max(open, close) + Math.random() * 60;
      candles.push({ open, close, high, low, volume: Math.random() * 110 + 15 });
      price = close;
    }

    let offset = 0;

    // ── Draw loop ────────────────────────────────────────────────────────────
    const draw = () => {
      const W = window.innerWidth;
      const H = window.innerHeight;

      // Background
      ctx.fillStyle = "#000000";
      ctx.fillRect(0, 0, W, H);

      // Glowing grid
      ctx.strokeStyle = "rgba(0,255,136,0.045)";
      ctx.lineWidth = 1;
      const GRID = 60;
      const gOff = offset % GRID;
      ctx.beginPath();
      for (let x = -gOff; x < W; x += GRID) { ctx.moveTo(x, 0); ctx.lineTo(x, H); }
      for (let y = 0; y < H; y += GRID)       { ctx.moveTo(0, y); ctx.lineTo(W, y); }
      ctx.stroke();

      // Live-tick the active candle
      const active = candles[candles.length - 1];
      const tick = (Math.random() - 0.5) * 55;
      active.close += tick;
      if (active.close < active.high) active.high = active.close;
      if (active.close > active.low)  active.low  = active.close;
      if (Math.random() > 0.65) active.volume += Math.random() * 6;

      // Advance scroll
      offset += SCROLL;
      if (offset >= STEP) {
        offset -= STEP;
        candles.shift();
        const prev = candles[candles.length - 1];
        // Soft drift to keep chart centred
        let drift = 0;
        if (prev.close < H * 0.22) drift =  Math.random() * 7;
        if (prev.close > H * 0.78) drift = -Math.random() * 7;
        const nextOpen = prev.close + drift;
        candles.push({ open: nextOpen, close: nextOpen, high: nextOpen, low: nextOpen, volume: 0 });
      }

      // Collect MA points while drawing candles
      const maPoints: { x: number; y: number }[] = [];

      ctx.save();
      for (let i = 0; i < candles.length; i++) {
        const c = candles[i];
        const x = i * STEP - offset;
        if (x + STEP < 0 || x > W) continue;

        const bull  = c.close <= c.open;
        const color = bull ? "#00ff88" : "#ff3355";
        const cxMid = Math.floor(x + STEP / 2) + 0.5;

        // Volume bar
        const volH = Math.min((c.volume / 230) * H * 0.18, H * 0.18);
        ctx.fillStyle = bull ? "rgba(0,255,136,0.10)" : "rgba(255,51,85,0.10)";
        ctx.fillRect(x + GAP / 2, H - volH, CW, volH);

        // Wick
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(cxMid, Math.floor(c.high));
        ctx.lineTo(cxMid, Math.floor(c.low));
        ctx.stroke();

        // Body with glow
        ctx.shadowBlur  = 9;
        ctx.shadowColor = color;
        ctx.fillStyle   = color;
        const bodyY = Math.min(c.open, c.close);
        const bodyH = Math.max(Math.abs(c.close - c.open), 2);
        ctx.fillRect(Math.floor(x + GAP / 2), Math.floor(bodyY), CW, Math.ceil(bodyH));
        ctx.shadowBlur = 0;

        // MA data
        if (i >= MA_PERIOD) {
          let sum = 0;
          for (let j = 0; j < MA_PERIOD; j++) sum += candles[i - j].close;
          maPoints.push({ x: cxMid, y: sum / MA_PERIOD });
        }
      }

      // Moving average line
      if (maPoints.length > 2) {
        ctx.beginPath();
        ctx.strokeStyle = "rgba(255,165,0,0.45)";
        ctx.lineWidth = 2;
        ctx.shadowBlur  = 14;
        ctx.shadowColor = "rgba(255,165,0,0.2)";
        ctx.moveTo(maPoints[0].x, maPoints[0].y);
        for (let i = 1; i < maPoints.length - 1; i++) {
          const mx = (maPoints[i].x + maPoints[i + 1].x) / 2;
          const my = (maPoints[i].y + maPoints[i + 1].y) / 2;
          ctx.quadraticCurveTo(maPoints[i].x, maPoints[i].y, mx, my);
        }
        ctx.lineTo(maPoints[maPoints.length - 1].x, maPoints[maPoints.length - 1].y);
        ctx.stroke();
        ctx.shadowBlur = 0;
      }
      ctx.restore();

      raf = requestAnimationFrame(draw);
    };

    draw();

    return () => {
      window.removeEventListener("resize", resize);
      cancelAnimationFrame(raf);
    };
  }, []);

  return (
    <div className="absolute inset-0 z-0 overflow-hidden bg-black">
      {/* Canvas */}
      <canvas ref={canvasRef} className="absolute inset-0" />

      {/* Scanlines */}
      <div
        className="absolute inset-0 pointer-events-none opacity-[0.08]"
        style={{
          background:
            "linear-gradient(rgba(18,16,16,0) 50%, rgba(0,0,0,0.28) 50%)",
          backgroundSize: "100% 3px",
        }}
      />

      {/* Noise texture */}
      <div
        className="absolute inset-0 pointer-events-none opacity-[0.035] mix-blend-screen"
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E")`,
          backgroundSize: "256px 256px",
        }}
      />

      {/* Vignette */}
      <div
        className="absolute inset-0 pointer-events-none"
        style={{
          background:
            "radial-gradient(ellipse at center, transparent 35%, rgba(0,0,0,0.82) 100%)",
        }}
      />
    </div>
  );
}
