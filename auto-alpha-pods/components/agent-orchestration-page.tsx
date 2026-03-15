"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  Download,
  Play,
  Plus,
  Sparkles,
  Terminal,
  X,
} from "lucide-react";

const ORCHESTRATION_API_BASE =
  process.env.NEXT_PUBLIC_BACKTEST_API_BASE_URL?.replace(/\/$/, "") ||
  "http://localhost:8000";

type Role = "manager" | "analyst";

interface AgentConfig {
  id: string;
  name: string;
  specialization: string;
  system_prompt: string;
  assets: string[];
}

interface OrchestrationEvent {
  id: string;
  ts: string;
  agent_id: string;
  agent_name: string;
  stage: string;
  message: string;
  result_ready: boolean;
}

interface OrchestrationRun {
  run_id: string;
  status: "queued" | "running" | "completed" | "failed";
  created_at: string;
  updated_at: string;
  config: {
    objective: string;
    manager: AgentConfig;
    analysts: AgentConfig[];
    start: string;
    end: string;
    initial_cash: number;
  };
  events: OrchestrationEvent[];
  results: Array<Record<string, unknown>>;
  report_markdown: string;
  error?: string | null;
}

interface FloorAgent {
  id: string;
  role: Role;
  name: string;
  specialization: string;
  x: number;
  y: number;
}

const defaultManager: AgentConfig = {
  id: "manager",
  name: "PM Athena",
  specialization: "Adversarial macro PM",
  system_prompt:
    "Be skeptical. Force clear hypotheses, explicit risks, and robust validation before approval.",
  assets: ["SPY", "TLT", "GLD", "QQQ"],
};

const defaultAnalysts: AgentConfig[] = [
  {
    id: "analyst-rates",
    name: "Rates Hawk",
    specialization: "Rates and curve regimes",
    system_prompt:
      "Focus on duration/inflation relationships and policy-cycle transitions.",
    assets: ["TLT", "IEF", "TIP", "SHY"],
  },
  {
    id: "analyst-risk",
    name: "Risk Sentinel",
    specialization: "Risk-on / risk-off cross-asset shifts",
    system_prompt:
      "Look for risk-regime shifts with defensiveness and downside control.",
    assets: ["SPY", "QQQ", "GLD", "HYG"],
  },
  {
    id: "analyst-inflation",
    name: "Inflation Scout",
    specialization: "Inflation and commodities",
    system_prompt:
      "Prioritize inflation momentum, real rates, and commodity hedging behavior.",
    assets: ["GLD", "SLV", "USO", "TIP"],
  },
];

function hashToPct(id: string, salt: number, min: number, max: number): number {
  let h = salt;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0;
  const span = max - min;
  return min + (h % 1000) / 1000 * span;
}

function stageTone(stage: string): string {
  if (stage === "result") return "text-emerald-300";
  if (stage === "backtest") return "text-sky-300";
  if (stage === "review") return "text-amber-300";
  if (stage === "news") return "text-cyan-300";
  if (stage === "critique") return "text-orange-300";
  if (stage === "revision") return "text-indigo-300";
  if (stage === "sphinx_cli") return "text-fuchsia-300";
  if (stage === "ideation" || stage === "proposal") return "text-violet-300";
  return "text-zinc-300";
}

export default function AgentOrchestrationPage() {
  const [objective, setObjective] = useState(
    "Find robust multi-asset alpha ideas with explicit downside protection and regime-awareness.",
  );
  const [manager, setManager] = useState<AgentConfig>(defaultManager);
  const [analysts, setAnalysts] = useState<AgentConfig[]>(defaultAnalysts);
  const [selectedNodeId, setSelectedNodeId] = useState<string>("manager");
  const [startingRun, setStartingRun] = useState(false);
  const [runId, setRunId] = useState<string | null>(null);
  const [runData, setRunData] = useState<OrchestrationRun | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [reportOpen, setReportOpen] = useState(false);

  const selectedRole: Role = selectedNodeId === "manager" ? "manager" : "analyst";
  const selectedAgent =
    selectedNodeId === "manager"
      ? manager
      : analysts.find((a) => a.id === selectedNodeId) || analysts[0];

  const figmaNodes = useMemo(() => {
    const managerNode = {
      id: manager.id,
      role: "manager" as const,
      name: manager.name,
      specialization: manager.specialization,
      x: 50,
      y: 17,
    };
    const analystNodes = analysts.map((analyst, idx) => {
      const spacing = 100 / (analysts.length + 1);
      return {
        id: analyst.id,
        role: "analyst" as const,
        name: analyst.name,
        specialization: analyst.specialization,
        x: spacing * (idx + 1),
        y: 72,
      };
    });
    return { managerNode, analystNodes };
  }, [manager, analysts]);

  const floorAgents: FloorAgent[] = useMemo(() => {
    const all = [
      {
        id: manager.id,
        role: "manager" as const,
        name: manager.name,
        specialization: manager.specialization,
      },
      ...analysts.map((a) => ({
        id: a.id,
        role: "analyst" as const,
        name: a.name,
        specialization: a.specialization,
      })),
    ];

    return all.map((agent, idx) => ({
      ...agent,
      x: hashToPct(agent.id, 97 + idx, 8, 88),
      y: hashToPct(agent.id, 193 + idx, 12, 78),
    }));
  }, [analysts, manager]);

  const latestByAgent = useMemo(() => {
    const map = new Map<string, OrchestrationEvent>();
    for (const event of runData?.events ?? []) map.set(event.agent_id, event);
    return map;
  }, [runData]);

  const resultAgents = useMemo(() => {
    const set = new Set<string>();
    for (const event of runData?.events ?? []) {
      if (event.result_ready) set.add(event.agent_id);
    }
    return set;
  }, [runData]);

  useEffect(() => {
    if (!runId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await fetch(
          `${ORCHESTRATION_API_BASE}/orchestration/runs/${runId}`,
          {
            cache: "no-store",
          },
        );
        if (!res.ok) throw new Error(`Poll failed (${res.status})`);
        const payload = (await res.json()) as OrchestrationRun;
        if (cancelled) return;
        setRunData(payload);
        if (payload.status === "completed" || payload.status === "failed") return;
        setTimeout(poll, 1200);
      } catch (err) {
        if (!cancelled) {
          setRunError(
            err instanceof Error ? err.message : "Failed to poll orchestration run.",
          );
          setTimeout(poll, 2000);
        }
      }
    };
    void poll();
    return () => {
      cancelled = true;
    };
  }, [runId]);

  const updateSelectedAgent = (
    field: keyof AgentConfig,
    value: string | string[],
  ) => {
    if (!selectedAgent) return;
    if (selectedRole === "manager") {
      setManager((prev) => ({ ...prev, [field]: value }));
      return;
    }
    setAnalysts((prev) =>
      prev.map((agent) =>
        agent.id === selectedAgent.id ? { ...agent, [field]: value } : agent,
      ),
    );
  };

  const addAnalyst = () => {
    const id = `analyst-${Date.now().toString(36)}`;
    const created: AgentConfig = {
      id,
      name: `Analyst ${analysts.length + 1}`,
      specialization: "Cross-asset strategist",
      system_prompt:
        "Produce testable ideas with citations, explicit risk controls, and a concise backtest prompt.",
      assets: ["SPY", "QQQ", "TLT"],
    };
    setAnalysts((prev) => [...prev, created]);
    setSelectedNodeId(id);
  };

  const removeSelectedAnalyst = () => {
    if (selectedRole !== "analyst" || !selectedAgent) return;
    const next = analysts.filter((a) => a.id !== selectedAgent.id);
    setAnalysts(next);
    setSelectedNodeId("manager");
  };

  const startRun = async () => {
    setStartingRun(true);
    setRunError(null);
    setReportOpen(false);
    try {
      const payload = {
        objective,
        manager,
        analysts,
        start: "2015-01-01",
        end: "2024-12-31",
        initial_cash: 100000,
      };
      const res = await fetch(`${ORCHESTRATION_API_BASE}/orchestration/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`Start run failed (${res.status})`);
      const started = (await res.json()) as { run_id: string };
      setRunId(started.run_id);
      setRunData(null);
    } catch (err) {
      setRunError(err instanceof Error ? err.message : "Failed to start run.");
    } finally {
      setStartingRun(false);
    }
  };

  const downloadReport = async () => {
    if (!runId) return;
    const res = await fetch(
      `${ORCHESTRATION_API_BASE}/orchestration/runs/${runId}/report.md`,
      { cache: "no-store" },
    );
    if (!res.ok) {
      setRunError(`Report not available (${res.status})`);
      return;
    }
    const markdown = await res.text();
    const blob = new Blob([markdown], { type: "text/markdown;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `orchestration-${runId}.md`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="min-h-screen bg-[#07070b] text-white">
      <header className="sticky top-0 z-40 border-b border-white/10 bg-[#07070b]/90 backdrop-blur-md">
        <div className="max-w-7xl mx-auto h-14 px-5 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link
              href="/dashboard"
              className="text-zinc-400 hover:text-zinc-100 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
            </Link>
            <p className="text-sm tracking-wide uppercase text-zinc-500">
              Agent Orchestration Studio
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={startRun}
              disabled={startingRun || analysts.length === 0}
              className="group relative overflow-hidden rounded-lg border border-emerald-400/40 bg-emerald-400/15 px-3 py-1.5 text-xs font-semibold text-emerald-200 hover:text-white hover:border-emerald-300/70 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              <span className="absolute inset-0 bg-linear-to-r from-transparent via-emerald-300/30 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-700" />
              <span className="relative inline-flex items-center gap-1.5">
                <Play className="w-3.5 h-3.5" />
                {startingRun ? "Starting..." : "Start Run"}
              </span>
            </button>
            <button
              type="button"
              onClick={() => setReportOpen((prev) => !prev)}
              disabled={!runData?.report_markdown}
              className="rounded-lg border border-white/15 bg-white/[0.03] px-3 py-1.5 text-xs font-semibold text-zinc-300 hover:text-white hover:border-white/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              Report
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-5 py-5 space-y-5">
        <section className="rounded-2xl border border-white/10 bg-linear-to-br from-white/[0.04] via-transparent to-emerald-500/10 p-4">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-[10px] uppercase tracking-[0.22em] text-zinc-500">
                Objective
              </p>
              <p className="text-sm text-zinc-300 mt-1">
                Configure analysts, manager, and run adversarial strategy orchestration.
              </p>
            </div>
            {runId && (
              <div className="text-right">
                <p className="text-[10px] text-zinc-500">Run ID</p>
                <p className="text-xs text-zinc-300 font-mono">{runId}</p>
              </div>
            )}
          </div>
          <textarea
            value={objective}
            onChange={(e) => setObjective(e.target.value)}
            className="mt-3 w-full min-h-[80px] rounded-xl border border-white/12 bg-black/40 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-emerald-400/50"
          />
          {runError && (
            <p className="text-xs text-rose-300 mt-2">Error: {runError}</p>
          )}
        </section>

        <section className="grid grid-cols-1 lg:grid-cols-[1.3fr_0.9fr] gap-5">
          <div className="rounded-2xl border border-white/10 bg-[#0a0b12] p-4">
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs uppercase tracking-wider text-zinc-500">
                Agent Map (Figma-like)
              </p>
              <button
                type="button"
                onClick={addAnalyst}
                className="inline-flex items-center gap-1 rounded-md border border-white/15 bg-white/[0.03] px-2 py-1 text-[11px] text-zinc-300 hover:text-white hover:border-white/30 transition-colors"
              >
                <Plus className="w-3 h-3" />
                Add Analyst
              </button>
            </div>

            <div className="relative h-[360px] rounded-xl border border-white/8 bg-[radial-gradient(circle_at_top,rgba(16,185,129,0.12),transparent_45%),radial-gradient(circle_at_bottom,rgba(56,189,248,0.09),transparent_45%)] overflow-hidden">
              <svg className="absolute inset-0 w-full h-full">
                <defs>
                  <marker
                    id="flowArrow"
                    viewBox="0 0 10 10"
                    refX="9"
                    refY="5"
                    markerWidth="5"
                    markerHeight="5"
                    orient="auto-start-reverse"
                  >
                    <path d="M 0 0 L 10 5 L 0 10 z" fill="#a1a1aa" />
                  </marker>
                </defs>
                {figmaNodes.analystNodes.map((node) => (
                  <line
                    key={`edge-${node.id}`}
                    x1={`${node.x}%`}
                    y1={`${node.y - 8}%`}
                    x2={`${figmaNodes.managerNode.x}%`}
                    y2={`${figmaNodes.managerNode.y + 7}%`}
                    stroke="rgba(161,161,170,0.6)"
                    strokeWidth="1.2"
                    strokeDasharray="4 4"
                    markerEnd="url(#flowArrow)"
                  />
                ))}
              </svg>

              <button
                type="button"
                onClick={() => setSelectedNodeId("manager")}
                className={`absolute -translate-x-1/2 -translate-y-1/2 w-44 rounded-xl border px-3 py-2 text-left transition-all ${
                  selectedNodeId === "manager"
                    ? "border-emerald-300/80 bg-emerald-300/15 shadow-[0_0_30px_rgba(16,185,129,0.25)]"
                    : "border-white/18 bg-black/45 hover:border-white/35"
                }`}
                style={{
                  left: `${figmaNodes.managerNode.x}%`,
                  top: `${figmaNodes.managerNode.y}%`,
                }}
              >
                <p className="text-[10px] uppercase tracking-wide text-emerald-300">
                  Manager
                </p>
                <p className="text-sm font-semibold text-zinc-100 truncate">
                  {manager.name}
                </p>
                <p className="text-[11px] text-zinc-400 truncate">
                  {manager.specialization}
                </p>
              </button>

              {figmaNodes.analystNodes.map((node) => (
                <button
                  type="button"
                  key={node.id}
                  onClick={() => setSelectedNodeId(node.id)}
                  className={`absolute -translate-x-1/2 -translate-y-1/2 w-44 rounded-xl border px-3 py-2 text-left transition-all ${
                    selectedNodeId === node.id
                      ? "border-sky-300/70 bg-sky-300/10 shadow-[0_0_24px_rgba(56,189,248,0.2)]"
                      : "border-white/15 bg-black/45 hover:border-white/30"
                  }`}
                  style={{ left: `${node.x}%`, top: `${node.y}%` }}
                >
                  <p className="text-[10px] uppercase tracking-wide text-sky-300">
                    Analyst
                  </p>
                  <p className="text-sm font-semibold text-zinc-100 truncate">
                    {node.name}
                  </p>
                  <p className="text-[11px] text-zinc-400 truncate">
                    {node.specialization}
                  </p>
                </button>
              ))}
            </div>
          </div>

          <div className="rounded-2xl border border-white/10 bg-[#0a0b12] p-4 space-y-3">
            <div className="flex items-center justify-between">
              <p className="text-xs uppercase tracking-wider text-zinc-500">
                Customize {selectedRole}
              </p>
              {selectedRole === "analyst" && (
                <button
                  type="button"
                  onClick={removeSelectedAnalyst}
                  className="text-[11px] px-2 py-1 rounded-md border border-rose-300/30 bg-rose-300/10 text-rose-200 hover:border-rose-300/60"
                >
                  Remove
                </button>
              )}
            </div>
            <label className="block text-[11px] text-zinc-500">
              Name
              <input
                value={selectedAgent?.name || ""}
                onChange={(e) => updateSelectedAgent("name", e.target.value)}
                className="mt-1 w-full rounded-lg border border-white/12 bg-black/45 px-2.5 py-1.5 text-sm text-zinc-100 outline-none focus:border-emerald-400/40"
              />
            </label>
            <label className="block text-[11px] text-zinc-500">
              Specialization
              <input
                value={selectedAgent?.specialization || ""}
                onChange={(e) =>
                  updateSelectedAgent("specialization", e.target.value)
                }
                className="mt-1 w-full rounded-lg border border-white/12 bg-black/45 px-2.5 py-1.5 text-sm text-zinc-100 outline-none focus:border-emerald-400/40"
              />
            </label>
            <label className="block text-[11px] text-zinc-500">
              Assets (comma separated)
              <input
                value={(selectedAgent?.assets || []).join(", ")}
                onChange={(e) =>
                  updateSelectedAgent(
                    "assets",
                    e.target.value
                      .split(",")
                      .map((asset) => asset.trim().toUpperCase())
                      .filter(Boolean),
                  )
                }
                className="mt-1 w-full rounded-lg border border-white/12 bg-black/45 px-2.5 py-1.5 text-sm text-zinc-100 outline-none focus:border-emerald-400/40"
              />
            </label>
            <label className="block text-[11px] text-zinc-500">
              Internal Prompt
              <textarea
                value={selectedAgent?.system_prompt || ""}
                onChange={(e) =>
                  updateSelectedAgent("system_prompt", e.target.value)
                }
                className="mt-1 w-full min-h-[110px] rounded-lg border border-white/12 bg-black/45 px-2.5 py-1.5 text-sm text-zinc-100 outline-none focus:border-emerald-400/40"
              />
            </label>
          </div>
        </section>

        <section className="grid grid-cols-1 xl:grid-cols-[1fr_1fr] gap-5">
          <div className="rounded-2xl border border-white/10 bg-[#0a0b12] p-4">
            <div className="flex items-center gap-2 mb-3">
              <Terminal className="w-4 h-4 text-emerald-300" />
              <p className="text-xs uppercase tracking-wider text-zinc-500">
                Trading Floor Log
              </p>
            </div>
            <div className="rounded-xl border border-white/8 bg-black/55 p-3 h-[320px] overflow-y-auto font-mono text-[11px] space-y-1.5">
              {(runData?.events || []).length === 0 ? (
                <p className="text-zinc-500">
                  Run not started. Configure agents, then click Start Run.
                </p>
              ) : (
                (runData?.events || []).map((event) => (
                  <div
                    key={event.id}
                    className={`leading-relaxed ${
                      event.stage === "sphinx_cli" ? "opacity-85 text-[10px]" : ""
                    }`}
                  >
                    <span className="text-zinc-600">
                      [{new Date(event.ts).toLocaleTimeString("en-US")}]
                    </span>{" "}
                    <span className="text-zinc-300">{event.agent_name}</span>{" "}
                    <span className={stageTone(event.stage)}>
                      ({event.stage.toUpperCase()})
                    </span>{" "}
                    <span className="text-zinc-400">{event.message}</span>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="rounded-2xl border border-white/10 bg-[#0a0b12] p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <Sparkles className="w-4 h-4 text-amber-300" />
                <p className="text-xs uppercase tracking-wider text-zinc-500">
                  Trading Floor (hover agents)
                </p>
              </div>
              <span className="text-[11px] text-zinc-500">
                {runData?.status || "idle"}
              </span>
            </div>
            <div className="relative h-[320px] rounded-xl border border-white/8 overflow-hidden bg-black">
              <div
                className="absolute inset-0 opacity-55"
                style={{
                  backgroundImage: "url('/trading_floor.png')",
                  backgroundSize: "cover",
                  backgroundPosition: "center",
                }}
              />
              <div className="absolute inset-0 bg-linear-to-b from-black/20 via-black/35 to-black/70" />
              {floorAgents.map((agent, idx) => {
                const latest = latestByAgent.get(agent.id);
                const hasResult = resultAgents.has(agent.id);
                return (
                  <button
                    key={agent.id}
                    type="button"
                    className="group absolute -translate-x-1/2 -translate-y-1/2"
                    style={{
                      left: `${agent.x}%`,
                      top: `${agent.y}%`,
                      animation: `floorBob ${1.4 + (idx % 3) * 0.45}s ease-in-out ${idx * 0.15}s infinite alternate`,
                    }}
                    onClick={() => {
                      if (hasResult) setReportOpen(true);
                    }}
                  >
                    <div
                      className={`relative h-5 w-5 border ${
                        agent.role === "manager"
                          ? "bg-emerald-300 border-emerald-100"
                          : "bg-sky-300 border-sky-100"
                      } shadow-[0_0_14px_rgba(255,255,255,0.35)]`}
                    >
                      {hasResult && (
                        <span className="absolute -right-1.5 -top-1.5 h-2.5 w-2.5 rounded-full bg-amber-300 border border-amber-100" />
                      )}
                    </div>
                    <div className="absolute left-1/2 -translate-x-1/2 mt-1 text-[10px] text-zinc-200 whitespace-nowrap">
                      {agent.name}
                    </div>
                    {latest && (
                      <div className="pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity absolute left-1/2 -translate-x-1/2 mt-6 w-52 rounded-md border border-white/20 bg-black/90 px-2 py-1 text-[10px] text-zinc-300 text-left">
                        <p className="text-zinc-100">{latest.agent_name}</p>
                        <p className="text-zinc-400 mt-0.5">{latest.message}</p>
                      </div>
                    )}
                  </button>
                );
              })}
            </div>
          </div>
        </section>
      </main>

      {reportOpen && (
        <aside className="fixed right-0 top-0 z-50 h-screen w-full max-w-xl border-l border-white/12 bg-[#07070b] shadow-2xl shadow-black/70 flex flex-col">
          <div className="h-14 px-4 border-b border-white/10 flex items-center justify-between">
            <p className="text-sm font-semibold text-zinc-100">
              Orchestration Report
            </p>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={downloadReport}
                disabled={!runData?.report_markdown}
                className="inline-flex items-center gap-1 rounded-md border border-white/20 bg-white/[0.03] px-2 py-1 text-xs text-zinc-300 hover:text-white hover:border-white/35 disabled:opacity-40"
              >
                <Download className="w-3 h-3" />
                Download .md
              </button>
              <button
                type="button"
                onClick={() => setReportOpen(false)}
                className="rounded-md border border-white/20 bg-white/[0.03] p-1 text-zinc-300 hover:text-white hover:border-white/35"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>
          <div className="flex-1 overflow-y-auto p-4">
            {runData?.report_markdown ? (
              <pre className="whitespace-pre-wrap text-xs leading-relaxed text-zinc-300 font-mono">
                {runData.report_markdown}
              </pre>
            ) : (
              <div className="h-full rounded-xl border border-white/10 bg-white/[0.02] flex items-center justify-center text-zinc-500 text-sm">
                Report will appear here when results are ready.
              </div>
            )}
          </div>
        </aside>
      )}

      <style>{`
        @keyframes floorBob {
          from { transform: translate(-50%, -52%); }
          to { transform: translate(-50%, -46%); }
        }
      `}</style>
    </div>
  );
}
