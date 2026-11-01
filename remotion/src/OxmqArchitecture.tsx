import React from "react";
import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
  Sequence,
} from "remotion";

// Color Palette
const COLORS = {
  bgDark: "#0B0F19",
  cardBg: "rgba(15, 23, 42, 0.82)",
  cardBorder: "rgba(56, 189, 248, 0.25)",
  cyan: "#38BDF8",
  blue: "#0284C7",
  emerald: "#10B981",
  ruby: "#EF4444",
  orange: "#F97316",
  amber: "#F59E0B",
  purple: "#A855F7",
  textPrimary: "#F8FAFC",
  textSecondary: "#94A3B8",
  textMuted: "#64748B",
};

// Scene 1: Hero & Intro (Frames 0 - 130)
const IntroScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const titleScale = spring({
    frame,
    fps,
    config: { damping: 12, mass: 0.8 },
  });

  const subtitleOpacity = interpolate(frame, [15, 35], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const badgesProgress = spring({
    frame: frame - 40,
    fps,
    config: { damping: 14 },
  });

  const sceneOpacity = interpolate(frame, [110, 130], [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        opacity: sceneOpacity,
        justifyContent: "center",
        alignItems: "center",
        flexDirection: "column",
        padding: "60px",
      }}
    >
      {/* Glowing Logo Icon */}
      <div
        style={{
          transform: `scale(${titleScale})`,
          marginBottom: "30px",
          position: "relative",
        }}
      >
        <div
          style={{
            width: "120px",
            height: "120px",
            borderRadius: "28px",
            background: "linear-gradient(135deg, #0284C7 0%, #38BDF8 50%, #10B981 100%)",
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            boxShadow: "0 0 50px rgba(56, 189, 248, 0.5), inset 0 0 20px rgba(255, 255, 255, 0.3)",
          }}
        >
          <svg width="68" height="68" viewBox="0 0 24 24" fill="none" stroke="#FFFFFF" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
          </svg>
        </div>
      </div>

      {/* Main Title */}
      <h1
        style={{
          fontSize: "76px",
          fontWeight: 800,
          letterSpacing: "-2px",
          margin: "0 0 16px 0",
          background: "linear-gradient(135deg, #FFFFFF 30%, #38BDF8 70%, #10B981 100%)",
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent",
          transform: `scale(${titleScale})`,
          textAlign: "center",
        }}
      >
        OxMQ Architecture
      </h1>

      {/* Subtitle */}
      <p
        style={{
          fontSize: "28px",
          color: COLORS.textSecondary,
          margin: "0 0 50px 0",
          opacity: subtitleOpacity,
          textAlign: "center",
          maxWidth: "1000px",
          lineHeight: 1.4,
        }}
      >
        Virtual Thread-Native Message Queue Engine for Java 21+ with{" "}
        <span style={{ color: COLORS.cyan, fontWeight: 700 }}>100% BullMQ Parity</span>
      </p>

      {/* Highlight Metrics Grid */}
      <div
        style={{
          display: "flex",
          gap: "24px",
          opacity: badgesProgress,
          transform: `translateY(${(1 - badgesProgress) * 30}px)`,
        }}
      >
        {[
          { icon: "⚡", label: "100,000+ msg/sec", sub: "Ultra-low Latency" },
          { icon: "🧵", label: "Java 21 Loom", sub: "10k+ Virtual Threads" },
          { icon: "🔄", label: "49 BullMQ Scripts", sub: "Exact Wire Interop" },
          { icon: "🛡️", label: "Strict Memory Ceiling", sub: "Zero-Loss Backpressure" },
        ].map((item, idx) => (
          <div
            key={idx}
            style={{
              padding: "20px 28px",
              background: "rgba(30, 41, 59, 0.7)",
              borderRadius: "16px",
              border: `1px solid ${COLORS.cardBorder}`,
              boxShadow: "0 10px 25px rgba(0, 0, 0, 0.4)",
              textAlign: "center",
              minWidth: "200px",
            }}
          >
            <div style={{ fontSize: "32px", marginBottom: "8px" }}>{item.icon}</div>
            <div style={{ fontSize: "20px", fontWeight: 700, color: COLORS.textPrimary }}>{item.label}</div>
            <div style={{ fontSize: "14px", color: COLORS.textMuted, marginTop: "4px" }}>{item.sub}</div>
          </div>
        ))}
      </div>
    </AbsoluteFill>
  );
};

// Scene 2: 3-Tier System Architecture (Frames 130 - 400)
const SystemTopologyScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const relFrame = frame;

  const headerSlide = spring({ frame: relFrame, fps, config: { damping: 14 } });
  const p1Slide = spring({ frame: relFrame - 15, fps, config: { damping: 14 } });
  const p2Slide = spring({ frame: relFrame - 30, fps, config: { damping: 14 } });
  const p3Slide = spring({ frame: relFrame - 45, fps, config: { damping: 14 } });

  const sceneOpacity = interpolate(relFrame, [250, 270], [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Animated beam pulses
  const pulseLeft = (Math.sin(relFrame * 0.15) + 1) / 2;
  const pulseRight = (Math.cos(relFrame * 0.15) + 1) / 2;

  return (
    <AbsoluteFill
      style={{
        opacity: sceneOpacity,
        padding: "60px 80px",
        flexDirection: "column",
      }}
    >
      {/* Section Header */}
      <div
        style={{
          transform: `translateY(${(1 - headerSlide) * -30}px)`,
          opacity: headerSlide,
          marginBottom: "40px",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "6px" }}>
          <span style={{ fontSize: "14px", fontWeight: 700, letterSpacing: "1.5px", color: COLORS.cyan, textTransform: "uppercase" }}>
            Architecture Overview
          </span>
          <span style={{ height: "2px", width: "40px", background: COLORS.cyan }}></span>
        </div>
        <h2 style={{ fontSize: "44px", fontWeight: 800, margin: 0, color: COLORS.textPrimary }}>
          High-Level End-to-End System Topology
        </h2>
      </div>

      {/* 3 Pillars Grid */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "stretch",
          gap: "36px",
          flex: 1,
          position: "relative",
        }}
      >
        {/* Tier 1: Application Layer */}
        <div
          style={{
            flex: 1,
            background: COLORS.cardBg,
            borderRadius: "20px",
            border: "1px solid rgba(56, 189, 248, 0.4)",
            padding: "30px",
            boxShadow: "0 20px 40px rgba(0, 0, 0, 0.5)",
            transform: `translateY(${(1 - p1Slide) * 40}px)`,
            opacity: p1Slide,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "14px", marginBottom: "20px" }}>
            <div style={{ background: "rgba(56, 189, 248, 0.2)", padding: "10px", borderRadius: "12px" }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke={COLORS.cyan} strokeWidth="2">
                <rect x="2" y="3" width="20" height="14" rx="2" ry="2" />
                <line x1="8" y1="21" x2="16" y2="21" />
                <line x1="12" y1="17" x2="12" y2="21" />
              </svg>
            </div>
            <div>
              <div style={{ fontSize: "13px", color: COLORS.cyan, fontWeight: 700, textTransform: "uppercase" }}>Layer 1</div>
              <div style={{ fontSize: "22px", fontWeight: 700, color: COLORS.textPrimary }}>Application &amp; Ingestion</div>
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: "12px", flex: 1, justifyContent: "center" }}>
            {[
              { name: "OxmqQueue<T>", desc: "Fast Java Producer & Bulk Ingest API" },
              { name: "FlowProducer", desc: "Hierarchical DAG Workflow Engine" },
              { name: "@OxmqListener", desc: "Spring Boot 3 Auto-Config Annotations" },
              { name: "QueueEvents", desc: "Low-overhead Redis Stream Events" },
            ].map((box, i) => (
              <div
                key={i}
                style={{
                  background: "rgba(30, 41, 59, 0.6)",
                  padding: "12px 16px",
                  borderRadius: "10px",
                  borderLeft: `3px solid ${COLORS.cyan}`,
                }}
              >
                <div style={{ fontSize: "16px", fontWeight: 700, color: COLORS.textPrimary, fontFamily: "monospace" }}>{box.name}</div>
                <div style={{ fontSize: "13px", color: COLORS.textSecondary }}>{box.desc}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Dynamic Connecting Arrow 1 -> 2 */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "center", width: "40px" }}>
          <div
            style={{
              width: "100%",
              height: "4px",
              background: `linear-gradient(90deg, ${COLORS.cyan}, ${COLORS.ruby})`,
              boxShadow: `0 0 ${10 + pulseLeft * 15}px ${COLORS.cyan}`,
              borderRadius: "2px",
            }}
          />
        </div>

        {/* Tier 2: Redis Engine & Lua Coordination */}
        <div
          style={{
            flex: 1.1,
            background: "rgba(24, 15, 30, 0.85)",
            borderRadius: "20px",
            border: "1px solid rgba(239, 68, 68, 0.4)",
            padding: "30px",
            boxShadow: "0 20px 40px rgba(0, 0, 0, 0.5)",
            transform: `translateY(${(1 - p2Slide) * 40}px)`,
            opacity: p2Slide,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "14px", marginBottom: "20px" }}>
            <div style={{ background: "rgba(239, 68, 68, 0.2)", padding: "10px", borderRadius: "12px" }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke={COLORS.ruby} strokeWidth="2">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
              </svg>
            </div>
            <div>
              <div style={{ fontSize: "13px", color: COLORS.ruby, fontWeight: 700, textTransform: "uppercase" }}>Layer 2</div>
              <div style={{ fontSize: "22px", fontWeight: 700, color: COLORS.textPrimary }}>Redis &amp; Atomic Lua Engine</div>
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
            <div style={{ background: "rgba(30, 20, 35, 0.8)", padding: "10px 14px", borderRadius: "10px", border: "1px dashed rgba(239, 68, 68, 0.5)" }}>
              <span style={{ fontSize: "12px", color: COLORS.ruby, fontWeight: 700 }}>49 BullMQ Lua Scripts (Zero Java Fork)</span>
              <div style={{ fontSize: "14px", color: COLORS.textSecondary, fontFamily: "monospace" }}>addStandardJob · moveToActive · moveToFinished</div>
            </div>

            {[
              { key: "bull:<queue>:wait", type: "List / ZSet (Prioritized)", color: COLORS.amber },
              { key: "bull:<queue>:active", type: "Hash Lock & Token Set", color: COLORS.orange },
              { key: "bull:<queue>:delayed", type: "Epoch Timestamp ZSet", color: COLORS.purple },
              { key: "bull:<queue>:events", type: "XADD Redis Stream", color: COLORS.cyan },
            ].map((k, i) => (
              <div
                key={i}
                style={{
                  background: "rgba(40, 25, 45, 0.6)",
                  padding: "10px 14px",
                  borderRadius: "8px",
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <span style={{ fontFamily: "monospace", fontSize: "14px", color: COLORS.textPrimary, fontWeight: 600 }}>{k.key}</span>
                <span style={{ fontSize: "12px", color: k.color, fontWeight: 700 }}>{k.type}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Dynamic Connecting Arrow 2 -> 3 */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "center", width: "40px" }}>
          <div
            style={{
              width: "100%",
              height: "4px",
              background: `linear-gradient(90deg, ${COLORS.ruby}, ${COLORS.emerald})`,
              boxShadow: `0 0 ${10 + pulseRight * 15}px ${COLORS.emerald}`,
              borderRadius: "2px",
            }}
          />
        </div>

        {/* Tier 3: Workers & Project Loom */}
        <div
          style={{
            flex: 1,
            background: COLORS.cardBg,
            borderRadius: "20px",
            border: "1px solid rgba(16, 185, 129, 0.4)",
            padding: "30px",
            boxShadow: "0 20px 40px rgba(0, 0, 0, 0.5)",
            transform: `translateY(${(1 - p3Slide) * 40}px)`,
            opacity: p3Slide,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "14px", marginBottom: "20px" }}>
            <div style={{ background: "rgba(16, 185, 129, 0.2)", padding: "10px", borderRadius: "12px" }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke={COLORS.emerald} strokeWidth="2">
                <circle cx="12" cy="12" r="10" />
                <polyline points="12 6 12 12 16 14" />
              </svg>
            </div>
            <div>
              <div style={{ fontSize: "13px", color: COLORS.emerald, fontWeight: 700, textTransform: "uppercase" }}>Layer 3</div>
              <div style={{ fontSize: "22px", fontWeight: 700, color: COLORS.textPrimary }}>Virtual Thread Workers</div>
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: "12px", flex: 1, justifyContent: "center" }}>
            {[
              { name: "OxmqWorker<T>", desc: "Loom Virtual Thread Per Task (~1 KB heap)" },
              { name: "OxmqBatchWorker<T>", desc: "Atomic Bulk Pop with maxBatchBytes safety" },
              { name: "Lock Watchdog", desc: "LockExtender heartbeat & Stalled Sentinel" },
              { name: "Bull-Board & Otel", desc: "W3C TraceContext & 100% UI Compatibility" },
            ].map((box, i) => (
              <div
                key={i}
                style={{
                  background: "rgba(30, 41, 59, 0.6)",
                  padding: "12px 16px",
                  borderRadius: "10px",
                  borderLeft: `3px solid ${COLORS.emerald}`,
                }}
              >
                <div style={{ fontSize: "16px", fontWeight: 700, color: COLORS.textPrimary, fontFamily: "monospace" }}>{box.name}</div>
                <div style={{ fontSize: "13px", color: COLORS.textSecondary }}>{box.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};

// Scene 3: Deep Dive: The Atomic Job Lifecycle in Motion (Frames 400 - 700)
const JobLifecycleScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const relFrame = frame;

  // Active step calculation (0 to 4)
  // Step 0 (0-60): Producer Enqueue
  // Step 1 (60-120): Redis Wait / Delayed
  // Step 2 (120-180): Worker Atomic Claim (moveToActive)
  // Step 3 (180-240): Virtual Thread Execution & Progress
  // Step 4 (240-300): Completion, Returnvalue & Event Stream
  const stepIndex = Math.min(4, Math.floor(relFrame / 60));

  const steps = [
    { title: "1. Enqueue Job", desc: "Producer calls queue.add(). Runs addStandardJob.lua. Atomically creates Hash & Wait List entry.", color: COLORS.cyan },
    { title: "2. Waiting & Scheduled", desc: "Job sits safely in bull:<queue>:wait or :delayed. Marker key wakes poller with 0 CPU spinning.", color: COLORS.amber },
    { title: "3. Atomic Claim", desc: "Worker executes moveToActive.lua. Atomically claims job, generates 30s lock, fires Stream event.", color: COLORS.orange },
    { title: "4. Virtual Thread Exec", desc: "Loom virtual thread executes processor. LockExtender heartbeat protects lock. Progress updates.", color: COLORS.emerald },
    { title: "5. Completed & Telemetry", desc: "Worker executes moveToFinished.lua. Saves returnvalue, cleans lock, updates Bull-Board UI.", color: COLORS.purple },
  ];

  const currentStep = steps[stepIndex];

  // Moving Job Packet position
  // Left: 15% (Enqueue) -> 35% (Redis) -> 55% (Active) -> 75% (Worker) -> 88% (Done)
  const packetX = interpolate(
    relFrame,
    [10, 60, 120, 180, 240, 290],
    [240, 620, 1000, 1380, 1680, 1680],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
  );

  const packetScale = spring({
    frame: relFrame % 60,
    fps,
    config: { damping: 10 },
  });

  const progressPercent = Math.min(100, Math.floor(interpolate(relFrame, [180, 240], [0, 100], { extrapolateLeft: "clamp", extrapolateRight: "clamp" })));

  const sceneOpacity = interpolate(relFrame, [280, 300], [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        opacity: sceneOpacity,
        padding: "50px 80px",
        flexDirection: "column",
      }}
    >
      {/* Header */}
      <div style={{ marginBottom: "30px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "6px" }}>
          <span style={{ fontSize: "14px", fontWeight: 700, letterSpacing: "1.5px", color: COLORS.emerald, textTransform: "uppercase" }}>
            Execution Engine Internals
          </span>
          <span style={{ height: "2px", width: "40px", background: COLORS.emerald }}></span>
        </div>
        <h2 style={{ fontSize: "40px", fontWeight: 800, margin: 0, color: COLORS.textPrimary }}>
          Real-Time Atomic Job Lifecycle
        </h2>
      </div>

      {/* Stepper Navigation */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "40px",
          background: "rgba(15, 23, 42, 0.6)",
          padding: "16px 24px",
          borderRadius: "16px",
          border: "1px solid rgba(255, 255, 255, 0.1)",
        }}
      >
        {steps.map((st, i) => {
          const isActive = i === stepIndex;
          const isPassed = i < stepIndex;
          return (
            <div
              key={i}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "12px",
                opacity: isActive ? 1 : isPassed ? 0.7 : 0.4,
                transform: isActive ? "scale(1.05)" : "scale(1)",
                transition: "all 0.3s ease",
              }}
            >
              <div
                style={{
                  width: "32px",
                  height: "32px",
                  borderRadius: "50%",
                  background: isActive ? st.color : isPassed ? COLORS.emerald : "#334155",
                  color: "#FFFFFF",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontWeight: 800,
                  fontSize: "14px",
                  boxShadow: isActive ? `0 0 16px ${st.color}` : "none",
                }}
              >
                {isPassed ? "✓" : i + 1}
              </div>
              <span
                style={{
                  fontSize: "16px",
                  fontWeight: isActive ? 700 : 500,
                  color: isActive ? st.color : COLORS.textPrimary,
                }}
              >
                {st.title.substring(3)}
              </span>
              {i < steps.length - 1 && (
                <span style={{ color: "#475569", marginLeft: "14px" }}>&rarr;</span>
              )}
            </div>
          );
        })}
      </div>

      {/* Main Animation Stage */}
      <div
        style={{
          flex: 1,
          position: "relative",
          background: "rgba(11, 15, 25, 0.9)",
          borderRadius: "24px",
          border: "1px solid rgba(255, 255, 255, 0.12)",
          padding: "40px",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          boxShadow: "inset 0 0 40px rgba(0, 0, 0, 0.8)",
        }}
      >
        {/* Track Line */}
        <div
          style={{
            position: "absolute",
            top: "140px",
            left: "140px",
            right: "140px",
            height: "4px",
            background: "rgba(255, 255, 255, 0.1)",
            borderRadius: "2px",
          }}
        >
          {/* Active Progress Line */}
          <div
            style={{
              height: "100%",
              width: `${(stepIndex / 4) * 100}%`,
              background: `linear-gradient(90deg, ${COLORS.cyan}, ${currentStep.color})`,
              boxShadow: `0 0 15px ${currentStep.color}`,
              transition: "width 0.4s ease",
            }}
          />
        </div>

        {/* 5 Stage Station Nodes */}
        <div style={{ display: "flex", justifyContent: "space-between", position: "relative", zIndex: 1, marginTop: "60px" }}>
          {[
            { label: "Producer", sub: "OxmqQueue.add()", color: COLORS.cyan },
            { label: "Redis :wait", sub: "FIFO List / ZSet", color: COLORS.amber },
            { label: "Redis :active", sub: "moveToActive.lua", color: COLORS.orange },
            { label: "Virtual Thread", sub: "Loom Processor", color: COLORS.emerald },
            { label: "Redis :completed", sub: "Bull-Board UI", color: COLORS.purple },
          ].map((node, idx) => {
            const isNodeActive = idx <= stepIndex;
            return (
              <div key={idx} style={{ textAlign: "center", width: "160px" }}>
                <div
                  style={{
                    width: "28px",
                    height: "28px",
                    borderRadius: "50%",
                    background: isNodeActive ? node.color : "#1E293B",
                    border: `3px solid ${isNodeActive ? "#FFFFFF" : "#475569"}`,
                    margin: "0 auto 14px auto",
                    boxShadow: isNodeActive ? `0 0 20px ${node.color}` : "none",
                  }}
                />
                <div style={{ fontSize: "16px", fontWeight: 700, color: isNodeActive ? COLORS.textPrimary : COLORS.textMuted }}>
                  {node.label}
                </div>
                <div style={{ fontSize: "12px", color: COLORS.textMuted, marginTop: "4px" }}>
                  {node.sub}
                </div>
              </div>
            );
          })}
        </div>

        {/* Animated Flying Job Card */}
        <div
          style={{
            position: "absolute",
            top: "80px",
            left: `${packetX - 110}px`,
            width: "220px",
            padding: "16px",
            background: "rgba(15, 23, 42, 0.95)",
            borderRadius: "14px",
            border: `2px solid ${currentStep.color}`,
            boxShadow: `0 10px 30px rgba(0, 0, 0, 0.8), 0 0 25px ${currentStep.color}66`,
            zIndex: 10,
            transform: `scale(${1 + packetScale * 0.05})`,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "12px", fontWeight: 700, color: currentStep.color, textTransform: "uppercase" }}>
              {stepIndex === 0 ? "NEW JOB" : stepIndex === 1 ? "WAITING" : stepIndex === 2 ? "CLAIMED" : stepIndex === 3 ? "RUNNING" : "COMPLETED"}
            </span>
            <span style={{ fontSize: "11px", color: COLORS.textMuted, fontFamily: "monospace" }}>#job-1024</span>
          </div>
          <div style={{ fontSize: "14px", fontWeight: 700, color: COLORS.textPrimary, marginBottom: "6px" }}>
            invoice-task
          </div>
          {stepIndex === 3 && (
            <div style={{ marginTop: "6px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "11px", color: COLORS.emerald, marginBottom: "4px" }}>
                <span>Progress</span>
                <span>{progressPercent}%</span>
              </div>
              <div style={{ width: "100%", height: "4px", background: "#334155", borderRadius: "2px", overflow: "hidden" }}>
                <div style={{ width: `${progressPercent}%`, height: "100%", background: COLORS.emerald }} />
              </div>
            </div>
          )}
          {stepIndex === 4 && (
            <div style={{ fontSize: "12px", color: COLORS.purple, marginTop: "4px", fontWeight: 600 }}>
              Return: "SUCCESS_200"
            </div>
          )}
        </div>

        {/* Bottom Explanatory Callout Banner */}
        <div
          style={{
            background: "rgba(30, 41, 59, 0.7)",
            padding: "20px 28px",
            borderRadius: "16px",
            borderLeft: `5px solid ${currentStep.color}`,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div>
            <div style={{ fontSize: "18px", fontWeight: 700, color: currentStep.color, marginBottom: "4px" }}>
              {currentStep.title}
            </div>
            <div style={{ fontSize: "15px", color: COLORS.textSecondary }}>
              {currentStep.desc}
            </div>
          </div>
          <div
            style={{
              padding: "8px 16px",
              background: "rgba(0, 0, 0, 0.4)",
              borderRadius: "8px",
              fontSize: "13px",
              fontFamily: "monospace",
              color: currentStep.color,
              border: `1px solid ${currentStep.color}44`,
            }}
          >
            ACID ATOMIC
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};

// Scene 4: Enterprise Powers & Resilience (Frames 700 - 850)
const EnterprisePowersScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const relFrame = frame;

  const headerSlide = spring({ frame: relFrame, fps, config: { damping: 14 } });

  const cards = [
    {
      title: "100% BullMQ Polyglot Parity",
      badge: "Cross-Language",
      color: COLORS.amber,
      icon: "🌐",
      desc: "Produce jobs in Java, consume in Node.js (or vice-versa). Instant out-of-the-box Bull-Board UI monitoring.",
      specs: ["Uses official 49 Lua scripts", "Identical Redis key topology", "Full Bull-Board dashboard"],
    },
    {
      title: "Hierarchical DAG Workflows",
      badge: "FlowProducer",
      color: COLORS.cyan,
      icon: "🌳",
      desc: "Assemble complex multi-tier parent-child dependency trees. Parent jobs automatically wake when all leaves finish.",
      specs: ["FlowProducer.add()", "Automatic waiting-children", "Partial failures handling"],
    },
    {
      title: "High-Throughput Batch Dequeue",
      badge: "OxmqBatchWorker",
      color: COLORS.emerald,
      icon: "📦",
      desc: "Atomically pop up to N jobs in a single Redis roundtrip with strict maxBatchBytes heap safety for bulk DB ingest.",
      specs: ["Multi-job atomic pop", "maxBatchBytes safety", "Virtual group affinity"],
    },
    {
      title: "Strict 200MB Redis Ceilings",
      badge: "Zero-Data-Loss",
      color: COLORS.ruby,
      icon: "🛡️",
      desc: "Application-layer backpressure via moveToDelayed(skipAttempt: true). Prevents catastrophic Redis OOM key drops.",
      specs: ["Adaptive rate limiting", "Tenant cooldown sentinel", "No LRU key evictions"],
    },
  ];

  const sceneOpacity = interpolate(relFrame, [130, 150], [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        opacity: sceneOpacity,
        padding: "60px 80px",
        flexDirection: "column",
      }}
    >
      <div
        style={{
          transform: `translateY(${(1 - headerSlide) * -30}px)`,
          opacity: headerSlide,
          marginBottom: "36px",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "6px" }}>
          <span style={{ fontSize: "14px", fontWeight: 700, letterSpacing: "1.5px", color: COLORS.purple, textTransform: "uppercase" }}>
            Enterprise Capabilities
          </span>
          <span style={{ height: "2px", width: "40px", background: COLORS.purple }}></span>
        </div>
        <h2 style={{ fontSize: "40px", fontWeight: 800, margin: 0, color: COLORS.textPrimary }}>
          Production Resilience &amp; Polyglot Power
        </h2>
      </div>

      {/* 2x2 Feature Grid */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gridGap: "24px",
          flex: 1,
        }}
      >
        {cards.map((c, idx) => {
          const cardSlide = spring({
            frame: relFrame - idx * 10,
            fps,
            config: { damping: 14 },
          });

          return (
            <div
              key={idx}
              style={{
                background: COLORS.cardBg,
                borderRadius: "20px",
                border: `1px solid ${c.color}44`,
                padding: "26px",
                boxShadow: "0 15px 35px rgba(0, 0, 0, 0.4)",
                transform: `translateY(${(1 - cardSlide) * 40}px)`,
                opacity: cardSlide,
                display: "flex",
                flexDirection: "column",
                justifyContent: "space-between",
              }}
            >
              <div>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "14px" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                    <span style={{ fontSize: "28px" }}>{c.icon}</span>
                    <span style={{ fontSize: "22px", fontWeight: 700, color: COLORS.textPrimary }}>{c.title}</span>
                  </div>
                  <span
                    style={{
                      fontSize: "12px",
                      fontWeight: 700,
                      color: c.color,
                      background: `${c.color}22`,
                      padding: "4px 12px",
                      borderRadius: "12px",
                      border: `1px solid ${c.color}44`,
                    }}
                  >
                    {c.badge}
                  </span>
                </div>
                <p style={{ fontSize: "15px", color: COLORS.textSecondary, lineHeight: 1.5, margin: "0 0 16px 0" }}>
                  {c.desc}
                </p>
              </div>

              <div style={{ display: "flex", gap: "10px" }}>
                {c.specs.map((s, si) => (
                  <span
                    key={si}
                    style={{
                      fontSize: "12px",
                      color: COLORS.textMuted,
                      background: "rgba(30, 41, 59, 0.6)",
                      padding: "4px 10px",
                      borderRadius: "6px",
                      border: "1px solid rgba(255, 255, 255, 0.05)",
                    }}
                  >
                    ✓ {s}
                  </span>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </AbsoluteFill>
  );
};

// Scene 5: Code Simplicity & Outro (Frames 850 - 960)
const CodeAndOutroScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const relFrame = frame;

  const contentSlide = spring({ frame: relFrame, fps, config: { damping: 14 } });

  return (
    <AbsoluteFill
      style={{
        padding: "60px 80px",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <div
        style={{
          transform: `scale(${contentSlide})`,
          opacity: contentSlide,
          width: "100%",
          maxWidth: "1400px",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
        }}
      >
        <h2 style={{ fontSize: "44px", fontWeight: 800, margin: "0 0 16px 0", color: COLORS.textPrimary, textAlign: "center" }}>
          Clean, Ergonomic Java 21+ APIs
        </h2>
        <p style={{ fontSize: "20px", color: COLORS.textSecondary, margin: "0 0 40px 0", textAlign: "center" }}>
          Production-grade reliability with zero boilerplate.
        </p>

        {/* 2 Code Cards */}
        <div style={{ display: "flex", gap: "30px", width: "100%", marginBottom: "40px" }}>
          {/* Producer Card */}
          <div
            style={{
              flex: 1,
              background: "#0D1117",
              borderRadius: "16px",
              border: "1px solid rgba(56, 189, 248, 0.3)",
              padding: "24px",
              boxShadow: "0 20px 40px rgba(0, 0, 0, 0.6)",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "16px" }}>
              <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: "#EF4444" }}></span>
              <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: "#F59E0B" }}></span>
              <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: "#10B981" }}></span>
              <span style={{ fontSize: "14px", color: COLORS.textSecondary, marginLeft: "10px", fontWeight: 600 }}>Producer.java</span>
            </div>
            <pre
              style={{
                margin: 0,
                fontSize: "15px",
                fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                color: "#E2E8F0",
                lineHeight: 1.6,
              }}
            >
              {`// Create Queue Producer
Queue<Invoice> queue = OxmqQueue.<Invoice>builder()
    .name("invoices")
    .redisUri("redis://localhost:6379")
    .build();

// Enqueue Job with retries & delay
Job<Invoice> job = queue.add(
    "generate-pdf",
    new Invoice("INV-2026-001"),
    JobOptions.builder()
        .attempts(3)
        .backoff(Backoff.exponential(1000))
        .build()
);`}
            </pre>
          </div>

          {/* Worker Card */}
          <div
            style={{
              flex: 1,
              background: "#0D1117",
              borderRadius: "16px",
              border: "1px solid rgba(16, 185, 129, 0.3)",
              padding: "24px",
              boxShadow: "0 20px 40px rgba(0, 0, 0, 0.6)",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "16px" }}>
              <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: "#EF4444" }}></span>
              <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: "#F59E0B" }}></span>
              <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: "#10B981" }}></span>
              <span style={{ fontSize: "14px", color: COLORS.textSecondary, marginLeft: "10px", fontWeight: 600 }}>Worker.java (Loom)</span>
            </div>
            <pre
              style={{
                margin: 0,
                fontSize: "15px",
                fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                color: "#E2E8F0",
                lineHeight: 1.6,
              }}
            >
              {`// Virtual Thread Worker Engine
Worker<Invoice> worker = OxmqWorker.<Invoice>builder()
    .queueName("invoices")
    .concurrency(10_000) // Project Loom Virtual Threads
    .processor(job -> {
        job.updateProgress(50);
        return generateInvoicePdf(job.getData());
    })
    .build();

worker.start();`}
            </pre>
          </div>
        </div>

        {/* Footer Banner */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            width: "100%",
            background: "linear-gradient(90deg, rgba(2, 132, 199, 0.2), rgba(16, 185, 129, 0.2))",
            border: `1px solid ${COLORS.cardBorder}`,
            padding: "20px 32px",
            borderRadius: "16px",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
            <span style={{ fontSize: "28px" }}>🚀</span>
            <div>
              <div style={{ fontSize: "18px", fontWeight: 800, color: COLORS.textPrimary }}>
                Ready to Supercharge Your Java Background Processing?
              </div>
              <div style={{ fontSize: "14px", color: COLORS.textSecondary }}>
                100% Free &amp; Open-Source under Apache-2.0 License
              </div>
            </div>
          </div>
          <div
            style={{
              padding: "10px 24px",
              background: COLORS.cyan,
              color: "#0B0F19",
              borderRadius: "10px",
              fontWeight: 800,
              fontSize: "16px",
              fontFamily: "monospace",
              boxShadow: `0 0 20px ${COLORS.cyan}88`,
            }}
          >
            github.com/gaurav10610/oxmq
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};

// Main Exported Composition
export const OxmqArchitecture: React.FC = () => {
  return (
    <AbsoluteFill
      style={{
        backgroundColor: COLORS.bgDark,
        fontFamily: "system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif",
        color: COLORS.textPrimary,
        overflow: "hidden",
      }}
    >
      {/* Dynamic Animated Background Mesh Grid */}
      <div
        style={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundImage: `
            radial-gradient(circle at 20% 30%, rgba(2, 132, 199, 0.15) 0%, transparent 50%),
            radial-gradient(circle at 80% 70%, rgba(16, 185, 129, 0.12) 0%, transparent 50%),
            linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px)
          `,
          backgroundSize: "100% 100%, 100% 100%, 60px 60px, 60px 60px",
        }}
      />

      {/* Top Navbar / Watermark */}
      <div
        style={{
          position: "absolute",
          top: "24px",
          left: "60px",
          right: "60px",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          zIndex: 100,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <div
            style={{
              width: "28px",
              height: "28px",
              borderRadius: "8px",
              background: "linear-gradient(135deg, #0284C7, #38BDF8)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: "#FFF",
              fontWeight: 900,
              fontSize: "14px",
            }}
          >
            O
          </div>
          <span style={{ fontSize: "20px", fontWeight: 800, letterSpacing: "-0.5px", color: COLORS.textPrimary }}>
            OxMQ <span style={{ color: COLORS.cyan, fontWeight: 500, fontSize: "14px" }}>v1.10.0</span>
          </span>
        </div>

        <div style={{ display: "flex", gap: "16px" }}>
          <span
            style={{
              fontSize: "12px",
              fontWeight: 700,
              padding: "4px 12px",
              borderRadius: "20px",
              background: "rgba(16, 185, 129, 0.15)",
              color: COLORS.emerald,
              border: "1px solid rgba(16, 185, 129, 0.3)",
            }}
          >
            ● BULLMQ WIRE PARITY
          </span>
          <span
            style={{
              fontSize: "12px",
              fontWeight: 700,
              padding: "4px 12px",
              borderRadius: "20px",
              background: "rgba(56, 189, 248, 0.15)",
              color: COLORS.cyan,
              border: "1px solid rgba(56, 189, 248, 0.3)",
            }}
          >
            JAVA 21 LOOM NATIVE
          </span>
        </div>
      </div>

      {/* 5 Distinct Sequenced Scenes */}
      <Sequence from={0} durationInFrames={130}>
        <IntroScene />
      </Sequence>

      <Sequence from={130} durationInFrames={270}>
        <SystemTopologyScene />
      </Sequence>

      <Sequence from={400} durationInFrames={300}>
        <JobLifecycleScene />
      </Sequence>

      <Sequence from={700} durationInFrames={150}>
        <EnterprisePowersScene />
      </Sequence>

      <Sequence from={850} durationInFrames={110}>
        <CodeAndOutroScene />
      </Sequence>
    </AbsoluteFill>
  );
};
