"use client";

import { useState, useEffect } from "react";
import { motion, AnimatePresence } from "motion/react";
import {
  Check,
  Loader2,
  Wallet,
  Users,
  ArrowRight,
  Dna,
  X,
} from "lucide-react";

interface Props {
  open: boolean;
  onClose: () => void;
}

type Step = "connect" | "config" | "spawning" | "done";

const STEPS = [
  { key: "connect", label: "Connect" },
  { key: "config", label: "Configure" },
  { key: "spawning", label: "Spawn" },
] as const;

const CONFIG_ITEMS = [
  {
    label: "Archetypes",
    value: "5 types",
    desc: "Momentum, Defensive, Volatility, Mean-Rev, Hybrid",
  },
  {
    label: "Starting Capital",
    value: "$100K",
    desc: "Paper trading allocation",
  },
  {
    label: "Genome Traits",
    value: "5 genes",
    desc: "Entry, Exit, Risk, Size, Indicator",
  },
  { label: "Broker", value: "Alpaca", desc: "Paper trading mode" },
];

export default function TradingDashboardModal({ open, onClose }: Props) {
  const [step, setStep] = useState<Step>("connect");
  const [agentCount, setAgentCount] = useState(40);
  const [connected, setConnected] = useState(false);
  const [progress, setProgress] = useState(0);

  // Reset state when re-opened
  useEffect(() => {
    if (open) {
      setStep("connect");
      setConnected(false);
      setProgress(0);
      setAgentCount(40);
    }
  }, [open]);

  const handleConnect = () => {
    setConnected(true);
    setTimeout(() => setStep("config"), 700);
  };

  const handleSpawn = () => {
    setStep("spawning");
    setProgress(0);

    // Simulate spawning progress
    let p = 0;
    const interval = setInterval(() => {
      p += Math.random() * 18 + 4;
      if (p >= 100) {
        p = 100;
        clearInterval(interval);
        setTimeout(() => setStep("done"), 300);
      }
      setProgress(Math.min(p, 100));
    }, 400);
  };

  const handleFinish = () => {
    onClose();
    window.open(
      "https://false-markets-trading.vercel.app/",
      "_blank",
      "noopener,noreferrer",
    );
  };

  const stepOrder: Step[] = ["connect", "config", "spawning", "done"];
  const currentIdx = stepOrder.indexOf(step);

  if (!open) return null;

  return (
    <AnimatePresence>
      <motion.div
        key="overlay"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
        className="fixed inset-0 z-[200] flex items-center justify-center bg-black/70 backdrop-blur-sm px-4"
        onClick={onClose}
      >
        <motion.div
          key="modal"
          initial={{ opacity: 0, scale: 0.95, y: 16 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 16 }}
          transition={{ duration: 0.22, ease: [0.16, 1, 0.3, 1] }}
          onClick={(e) => e.stopPropagation()}
          className="relative w-full max-w-lg rounded-xl border border-white/[0.10] bg-[#0a0a0a] shadow-2xl overflow-hidden"
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-white/[0.07] px-6 py-4">
            <div className="flex items-center gap-2.5">
              <Dna className="h-4 w-4 text-violet-400" />
              <h2 className="text-base font-bold text-white tracking-tight">
                Launch Your Evolution
              </h2>
            </div>
            <button
              onClick={onClose}
              className="text-zinc-600 hover:text-zinc-300 transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Steps indicator */}
          <div className="flex items-center gap-2 px-6 pt-5 pb-2">
            {STEPS.map((s, i) => {
              const stepIdx = i;
              const isActive = stepIdx <= currentIdx;
              const isDone = stepIdx < currentIdx || step === "done";

              return (
                <div key={s.key} className="flex items-center gap-2 flex-1">
                  <div
                    className={`flex h-6 w-6 items-center justify-center rounded-full text-[10px] font-bold transition-all duration-300 ${
                      isDone
                        ? "bg-violet-500 text-white"
                        : isActive
                          ? "border-2 border-violet-500 text-violet-400"
                          : "border border-white/[0.12] text-zinc-600"
                    }`}
                  >
                    {isDone ? <Check className="h-3 w-3" /> : i + 1}
                  </div>
                  <span
                    className={`text-xs font-mono transition-colors ${
                      isActive ? "text-zinc-200" : "text-zinc-600"
                    }`}
                  >
                    {s.label}
                  </span>
                  {i < STEPS.length - 1 && (
                    <div
                      className={`flex-1 h-px transition-colors duration-500 ${
                        isDone ? "bg-violet-500/60" : "bg-white/[0.07]"
                      }`}
                    />
                  )}
                </div>
              );
            })}
          </div>

          {/* Content */}
          <div className="px-6 py-6 min-h-[280px] flex flex-col">
            <AnimatePresence mode="wait">
              {/* Step 1: Connect */}
              {step === "connect" && (
                <motion.div
                  key="connect"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  transition={{ duration: 0.18 }}
                  className="flex flex-col items-center text-center gap-5 flex-1 justify-center"
                >
                  <div className="h-16 w-16 rounded-2xl bg-amber-500/10 border border-amber-500/20 flex items-center justify-center">
                    <Wallet className="h-8 w-8 text-amber-400" />
                  </div>
                  <div>
                    <h3 className="text-base font-bold text-white">
                      Connect Alpaca Wallet
                    </h3>
                    <p className="mt-1.5 text-sm text-zinc-400 max-w-xs leading-relaxed">
                      Your agents will paper-trade through Alpaca's brokerage
                      API. Connect to enable live market execution.
                    </p>
                  </div>
                  <button
                    onClick={handleConnect}
                    disabled={connected}
                    className="flex items-center gap-2 mt-1 px-6 py-2.5 rounded-lg bg-violet-500/15 border border-violet-500/30 text-sm font-semibold text-violet-300 hover:bg-violet-500/25 hover:border-violet-500/50 transition-all disabled:opacity-60 disabled:cursor-not-allowed min-w-[180px] justify-center"
                  >
                    {connected ? (
                      <>
                        <Check className="h-4 w-4" /> Connected
                      </>
                    ) : (
                      <>
                        <Wallet className="h-4 w-4" /> Connect Alpaca
                      </>
                    )}
                  </button>
                </motion.div>
              )}

              {/* Step 2: Configure */}
              {step === "config" && (
                <motion.div
                  key="config"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  transition={{ duration: 0.18 }}
                  className="flex flex-col gap-5 flex-1"
                >
                  <div className="text-center">
                    <h3 className="text-base font-bold text-white">
                      Configure Population
                    </h3>
                    <p className="mt-1 text-sm text-zinc-400">
                      Choose how many AI trading agents to spawn in Generation
                      0.
                    </p>
                  </div>

                  {/* Agent count slider */}
                  <div className="rounded-lg border border-white/[0.08] bg-white/[0.03] p-4">
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center gap-2">
                        <Users className="h-4 w-4 text-violet-400" />
                        <span className="text-sm font-medium text-zinc-300">
                          Agent Count
                        </span>
                      </div>
                      <span className="text-2xl font-bold font-mono text-violet-300">
                        {agentCount}
                      </span>
                    </div>
                    <input
                      type="range"
                      min={10}
                      max={100}
                      step={5}
                      value={agentCount}
                      onChange={(e) => setAgentCount(Number(e.target.value))}
                      className="w-full accent-violet-500 cursor-pointer"
                    />
                    <div className="flex justify-between mt-1.5 text-[10px] font-mono text-zinc-600">
                      <span>10</span>
                      <span>50</span>
                      <span>100</span>
                    </div>
                  </div>

                  {/* Summary grid */}
                  <div className="grid grid-cols-2 gap-2.5">
                    {CONFIG_ITEMS.map((item) => (
                      <div
                        key={item.label}
                        className="rounded-lg border border-white/[0.07] bg-white/[0.02] p-3"
                      >
                        <p className="text-[10px] font-mono text-zinc-600 uppercase tracking-wide">
                          {item.label}
                        </p>
                        <p className="text-sm font-bold font-mono text-zinc-200 mt-0.5">
                          {item.value}
                        </p>
                        <p className="text-[10px] text-zinc-600 mt-0.5">
                          {item.desc}
                        </p>
                      </div>
                    ))}
                  </div>

                  <button
                    onClick={handleSpawn}
                    className="flex items-center justify-center gap-2 w-full py-2.5 rounded-lg bg-violet-500/15 border border-violet-500/30 text-sm font-semibold text-violet-300 hover:bg-violet-500/25 hover:border-violet-500/50 transition-all"
                  >
                    Spawn {agentCount} Agents
                    <ArrowRight className="h-4 w-4" />
                  </button>
                </motion.div>
              )}

              {/* Step 3: Spawning */}
              {step === "spawning" && (
                <motion.div
                  key="spawning"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.18 }}
                  className="flex flex-col items-center text-center gap-5 flex-1 justify-center"
                >
                  <Loader2 className="h-10 w-10 text-violet-400 animate-spin" />
                  <div>
                    <h3 className="text-base font-bold text-white">
                      Spawning Agents…
                    </h3>
                    <p className="mt-1.5 text-sm text-zinc-400 max-w-xs leading-relaxed">
                      AI is generating {agentCount} unique trading strategies
                      with diverse genomes.
                    </p>
                  </div>
                  <div className="w-full max-w-xs">
                    <div className="h-1.5 rounded-full bg-white/[0.06] overflow-hidden">
                      <motion.div
                        className="h-full rounded-full bg-violet-500"
                        animate={{ width: `${progress}%` }}
                        transition={{ duration: 0.3, ease: "easeOut" }}
                      />
                    </div>
                    <p className="mt-2 text-xs font-mono text-zinc-600">
                      {Math.round(progress)}% complete
                    </p>
                  </div>
                </motion.div>
              )}

              {/* Step 4: Done */}
              {step === "done" && (
                <motion.div
                  key="done"
                  initial={{ opacity: 0, scale: 0.92 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.22, ease: [0.16, 1, 0.3, 1] }}
                  className="flex flex-col items-center text-center gap-5 flex-1 justify-center"
                >
                  <div className="h-16 w-16 rounded-full bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center">
                    <Check className="h-8 w-8 text-emerald-400" />
                  </div>
                  <div>
                    <h3 className="text-base font-bold text-white">
                      Population Ready!
                    </h3>
                    <p className="mt-1.5 text-sm text-zinc-400 max-w-xs leading-relaxed">
                      {agentCount} agents are alive and ready to evolve. Head to
                      the dashboard to begin.
                    </p>
                  </div>
                  <button
                    onClick={handleFinish}
                    className="flex items-center gap-2 mt-1 px-6 py-2.5 rounded-lg bg-emerald-500/15 border border-emerald-500/30 text-sm font-semibold text-emerald-300 hover:bg-emerald-500/25 hover:border-emerald-500/50 transition-all min-w-[200px] justify-center"
                  >
                    Enter Dashboard
                    <ArrowRight className="h-4 w-4" />
                  </button>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>
  );
}
