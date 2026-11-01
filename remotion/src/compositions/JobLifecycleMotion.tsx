import React from "react";
import { AbsoluteFill, useCurrentFrame, interpolate } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const JobLifecycleMotion: React.FC = () => {
  const frame = useCurrentFrame();

  const nodes = [
    { id: "waiting", label: "WAITING", key: "bull:<q>:wait", desc: "FIFO queue list", x: 320, y: 280, color: THEME.cyan },
    { id: "active", label: "ACTIVE", key: "bull:<q>:active", desc: "Loom Virtual Thread lock", x: 890, y: 280, color: THEME.cyan },
    { id: "completed", label: "COMPLETED", key: "bull:<q>:completed", desc: "Saved returnvalue hash", x: 1460, y: 280, color: THEME.emerald },
    { id: "failed", label: "FAILED", key: "bull:<q>:failed", desc: "Retries exhausted / crash", x: 890, y: 560, color: THEME.ruby },
    { id: "delayed", label: "DELAYED", key: "bull:<q>:delayed", desc: "Timestamp sorted set", x: 320, y: 560, color: THEME.purple },
    { id: "waiting-children", label: "WAITING_CHILDREN", key: "bull:<q>:waiting-children", desc: "FlowProducer DAG tree", x: 1460, y: 560, color: THEME.indigo },
  ];

  // Moving Job Packet Logic
  let packetX = 320;
  let packetY = 280;
  let currentAction = "1. Producer enqueues job via add.lua -> placed into 'wait' list";
  let activeScript = "addJob-8.lua (Atomic Ingestion)";
  let activeColor = THEME.cyan;

  if (frame < 72) {
    // Waiting
    packetX = 320;
    packetY = 280;
    currentAction = "1. Producer enqueues job via add.lua -> placed into 'wait' list";
    activeScript = "addJob-8.lua (Atomic Ingestion)";
    activeColor = THEME.cyan;
  } else if (frame < 144) {
    // Phase 1: Waiting to Active
    const p = interpolate(frame, [72, 120], [0, 1], { extrapolateRight: "clamp" });
    packetX = interpolate(p, [0, 1], [320, 890]);
    packetY = 280;
    currentAction = "2. Worker calls moveToActive.lua: atomic pop from 'wait', moves to 'active', locks for 30s";
    activeScript = "moveToActive.lua (Token Lock Claim)";
    activeColor = THEME.cyan;
  } else if (frame < 216) {
    // Phase 2: Active execution & heartbeat
    packetX = 890;
    packetY = 280;
    const progress = Math.min(100, Math.floor(interpolate(frame, [144, 200], [0, 100], { extrapolateRight: "clamp" })));
    currentAction = `3. Project Loom Virtual Thread executes processor. Progress: ${progress}%. LockExtender heartbeat active.`;
    activeScript = "LockExtender heartbeat (PEXPIRE lock 30s)";
    activeColor = THEME.cyan;
  } else if (frame < 288) {
    // Phase 3: Active to Completed
    const p = interpolate(frame, [216, 264], [0, 1], { extrapolateRight: "clamp" });
    packetX = interpolate(p, [0, 1], [890, 1460]);
    packetY = 280;
    currentAction = "4. Worker calls moveToFinished.lua: job moves to 'completed', returnvalue stored, lock deleted";
    activeScript = "moveToFinished.lua (Status: Completed)";
    activeColor = THEME.emerald;
  } else {
    // Phase 4: Alternative branches (Delayed, Failed, DAG)
    packetX = 1460;
    packetY = 280;
    currentAction = "5. Auxiliary transitions: retry failed jobs with backoff, or delay via moveToDelayed";
    activeScript = "moveToDelayed.lua / retryJobs.lua";
    activeColor = THEME.purple;
  }

  // Pulse
  const pulse = (Math.sin(frame * 0.2) + 1) / 2;

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "45px 65px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Job Lifecycle & State Machine"
        title="OxMQ Atomic Job State Transitions"
        subtitle="100% Wire-Compatible state machine executing official BullMQ Lua scripts with microsecond ACID atomicity"
        badgeText="49 BULLMQ LUA SCRIPTS"
        badgeColor={THEME.emerald}
      />

      {/* Main Graph Canvas */}
      <div
        style={{
          flex: 1,
          position: "relative",
          background: THEME.cardBg,
          borderRadius: "22px",
          border: `1.5px solid ${THEME.cardBorderSubtle}`,
          padding: "30px",
          boxShadow: "0 20px 50px rgba(0, 0, 0, 0.6)",
          zIndex: 1,
        }}
      >
        {/* Connecting SVG Curves */}
        <svg
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            width: "100%",
            height: "100%",
            pointerEvents: "none",
          }}
        >
          {/* Waiting -> Active */}
          <line x1="500" y1="280" x2="710" y2="280" stroke={THEME.cyan} strokeWidth="4" strokeDasharray="8 6" opacity="0.85" />
          {/* Active -> Completed */}
          <line x1="1070" y1="280" x2="1280" y2="280" stroke={THEME.emerald} strokeWidth="4" strokeDasharray="8 6" opacity="0.85" />
          {/* Active -> Failed */}
          <line x1="890" y1="365" x2="890" y2="475" stroke={THEME.ruby} strokeWidth="3" strokeDasharray="6 6" opacity="0.7" />
          {/* Waiting -> Delayed */}
          <line x1="320" y1="365" x2="320" y2="475" stroke={THEME.purple} strokeWidth="3" strokeDasharray="6 6" opacity="0.7" />
          {/* Active -> Waiting Children */}
          <path d="M 1070 320 C 1200 400, 1280 480, 1340 520" fill="none" stroke={THEME.indigo} strokeWidth="3" strokeDasharray="6 6" opacity="0.7" />
          {/* Delayed -> Waiting loop */}
          <path d="M 450 560 C 560 560, 560 360, 470 310" fill="none" stroke={THEME.amber} strokeWidth="3" strokeDasharray="6 6" opacity="0.7" />
        </svg>

        {/* 6 State Nodes */}
        {nodes.map((node) => {
          const isNodeActive =
            (node.id === "waiting" && frame < 72) ||
            (node.id === "active" && frame >= 72 && frame < 216) ||
            (node.id === "completed" && frame >= 216 && frame < 288) ||
            (frame >= 288);

          return (
            <div
              key={node.id}
              style={{
                position: "absolute",
                left: `${node.x - 175}px`,
                top: `${node.y - 82}px`,
                width: "350px",
                padding: "20px 24px",
                background: isNodeActive ? "rgba(30, 41, 59, 0.96)" : "rgba(15, 23, 42, 0.8)",
                borderRadius: "18px",
                border: `2.5px solid ${isNodeActive ? node.color : "rgba(255, 255, 255, 0.14)"}`,
                boxShadow: isNodeActive ? `0 0 35px ${node.color}66, 0 12px 24px rgba(0,0,0,0.5)` : "0 8px 16px rgba(0,0,0,0.3)",
                transition: "all 0.3s ease",
                zIndex: 2,
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                <span style={{ fontSize: "22px", fontWeight: 800, color: node.color, letterSpacing: "0.5px" }}>
                  {node.label}
                </span>
                <span
                  style={{
                    width: "14px",
                    height: "14px",
                    borderRadius: "50%",
                    background: isNodeActive ? node.color : "#475569",
                    boxShadow: isNodeActive ? `0 0 14px ${node.color}` : "none",
                  }}
                />
              </div>
              <div style={{ fontSize: "16px", fontFamily: THEME.fontMono, color: THEME.textPrimary, marginBottom: "6px", fontWeight: 700 }}>
                {node.key}
              </div>
              <div style={{ fontSize: "15px", color: THEME.textMuted, lineHeight: "1.4" }}>{node.desc}</div>
            </div>
          );
        })}

        {/* Moving Animated Job Packet */}
        <div
          style={{
            position: "absolute",
            left: `${packetX - 110}px`,
            top: `${packetY - 45}px`,
            width: "220px",
            padding: "12px 18px",
            background: "rgba(11, 15, 25, 0.96)",
            borderRadius: "14px",
            border: `2.5px solid ${activeColor}`,
            boxShadow: `0 0 30px ${activeColor}99, 0 10px 25px rgba(0,0,0,0.7)`,
            zIndex: 10,
            transform: `scale(${1 + pulse * 0.06})`,
            pointerEvents: "none",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: "15px", fontWeight: 800, color: activeColor }}>JOB #1024</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary, fontFamily: THEME.fontMono, fontWeight: 600 }}>attempts: 1</span>
          </div>
          <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.textPrimary, marginTop: "4px" }}>
            send-invoice
          </div>
        </div>

        {/* Explanatory Bottom Ribbon */}
        <div
          style={{
            position: "absolute",
            bottom: "24px",
            left: "30px",
            right: "30px",
            background: "rgba(15, 23, 42, 0.95)",
            borderRadius: "16px",
            borderLeft: `6px solid ${activeColor}`,
            border: `1px solid rgba(255,255,255,0.12)`,
            padding: "18px 28px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontSize: "19px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "4px" }}>
              {currentAction}
            </div>
            <div style={{ fontSize: "16px", color: THEME.textSecondary }}>
              Executed Script: <span style={{ fontFamily: THEME.fontMono, color: activeColor, fontWeight: 700 }}>{activeScript}</span>
            </div>
          </div>
          <div
            style={{
              padding: "8px 18px",
              borderRadius: "10px",
              background: `${activeColor}26`,
              border: `1.5px solid ${activeColor}55`,
              color: activeColor,
              fontWeight: 800,
              fontSize: "14px",
              letterSpacing: "0.5px",
            }}
          >
            ATOMIC REDIS LUA
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
