import React from "react";
import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const JobLifecycleMotion: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // 360 frames total (12 seconds @ 30 fps)
  // Phase 1 (0-60): WAITING state (addStandardJob.lua / addDelayedJob.lua)
  // Phase 2 (60-140): ACTIVE state (moveToActive.lua, LockExtender heartbeat, Loom Thread)
  // Phase 3 (140-220): Execution & Progress reporting (job.updateProgress)
  // Phase 4 (220-290): COMPLETED transition (moveToFinished.lua, returnvalue saved)
  // Phase 5 (290-360): Auxiliary paths (DELAYED, FAILED with backoff, WAITING_CHILDREN)
  const phase = Math.min(4, Math.floor(frame / 72));

  // Animated packet position
  // 6 state nodes in a circular / flow topology
  const nodes = [
    { id: "waiting", label: "WAITING", key: "bull:<q>:wait", color: THEME.amber, x: 260, y: 320, desc: "Ready for workers" },
    { id: "active", label: "ACTIVE", key: "bull:<q>:active", color: THEME.cyan, x: 680, y: 320, desc: "Loom Virtual Thread" },
    { id: "completed", label: "COMPLETED", key: "bull:<q>:completed", color: THEME.emerald, x: 1100, y: 320, desc: "Return value stored" },
    { id: "delayed", label: "DELAYED", key: "bull:<q>:delayed", color: THEME.purple, x: 260, y: 640, desc: "Timestamp ZSet" },
    { id: "failed", label: "FAILED", key: "bull:<q>:failed", color: THEME.ruby, x: 680, y: 640, desc: "Retry & Backoff" },
    { id: "waiting-children", label: "WAITING_CHILDREN", key: "bull:<q>:w-children", color: THEME.indigo, x: 1100, y: 640, desc: "Parent DAG wait" },
  ];

  // Packet animation
  let packetX = 260;
  let packetY = 320;
  let currentAction = "Enqueued into bull:<queue>:wait via addStandardJob.lua";
  let activeScript = "addStandardJob.lua";
  let activeColor = THEME.amber;

  if (frame < 72) {
    // Phase 0: Enqueue to Waiting
    const p = interpolate(frame, [0, 50], [0, 1], { extrapolateRight: "clamp" });
    packetX = interpolate(p, [0, 1], [100, 260]);
    packetY = 320;
    currentAction = "1. Producer enqueues job via addStandardJob.lua -> added to 'wait' List";
    activeScript = "addStandardJob.lua (ACID atomic)";
    activeColor = THEME.amber;
  } else if (frame < 144) {
    // Phase 1: Waiting to Active
    const p = interpolate(frame, [72, 120], [0, 1], { extrapolateRight: "clamp" });
    packetX = interpolate(p, [0, 1], [260, 680]);
    packetY = 320;
    currentAction = "2. Worker calls moveToActive.lua: atomics pop from 'wait', moves to 'active', locks for 30s";
    activeScript = "moveToActive.lua (Token Lock Claim)";
    activeColor = THEME.cyan;
  } else if (frame < 216) {
    // Phase 2: Active execution & heartbeat
    packetX = 680;
    packetY = 320;
    const progress = Math.min(100, Math.floor(interpolate(frame, [144, 200], [0, 100], { extrapolateRight: "clamp" })));
    currentAction = `3. Project Loom Virtual Thread executes processor. Progress: ${progress}%. LockExtender heartbeat active.`;
    activeScript = "LockExtender heartbeat (PEXPIRE lock 30s)";
    activeColor = THEME.cyan;
  } else if (frame < 288) {
    // Phase 3: Active to Completed
    const p = interpolate(frame, [216, 264], [0, 1], { extrapolateRight: "clamp" });
    packetX = interpolate(p, [0, 1], [680, 1100]);
    packetY = 320;
    currentAction = "4. Worker calls moveToFinished.lua: job moves to 'completed', returnvalue stored, lock deleted";
    activeScript = "moveToFinished.lua (Status: Completed)";
    activeColor = THEME.emerald;
  } else {
    // Phase 4: Alternative branches (Delayed, Failed, DAG)
    packetX = 1100;
    packetY = 320;
    currentAction = "5. Auxiliary transitions: retry failed jobs with exponential backoff, or delay via moveToDelayed";
    activeScript = "moveToDelayed.lua / retryJobs.lua";
    activeColor = THEME.purple;
  }

  // Pulse
  const pulse = (Math.sin(frame * 0.2) + 1) / 2;

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Job Lifecycle & State Machine"
        title="OxMQ Atomic Job State Transitions"
        subtitle="100% Wire-Compatible state machine executing official BullMQ Lua scripts with microsecond ACID guarantees"
        badgeText="49 BULLMQ LUA SCRIPTS"
        badgeColor={THEME.emerald}
      />

      {/* Main Graph Canvas */}
      <div
        style={{
          flex: 1,
          position: "relative",
          background: THEME.cardBg,
          borderRadius: "20px",
          border: `1px solid ${THEME.cardBorderSubtle}`,
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
          <line x1="390" y1="320" x2="550" y2="320" stroke={THEME.cyan} strokeWidth="3" strokeDasharray="6 4" opacity="0.8" />
          {/* Active -> Completed */}
          <line x1="810" y1="320" x2="970" y2="320" stroke={THEME.emerald} strokeWidth="3" strokeDasharray="6 4" opacity="0.8" />
          {/* Active -> Failed */}
          <line x1="680" y1="390" x2="680" y2="570" stroke={THEME.ruby} strokeWidth="2.5" strokeDasharray="4 4" opacity="0.6" />
          {/* Waiting -> Delayed */}
          <line x1="260" y1="390" x2="260" y2="570" stroke={THEME.purple} strokeWidth="2.5" strokeDasharray="4 4" opacity="0.6" />
          {/* Active -> Waiting Children */}
          <path d="M 810 350 C 900 450, 950 550, 970 640" fill="none" stroke={THEME.indigo} strokeWidth="2" strokeDasharray="4 4" opacity="0.6" />
          {/* Delayed -> Waiting loop */}
          <path d="M 390 640 C 450 640, 450 400, 390 350" fill="none" stroke={THEME.amber} strokeWidth="2" strokeDasharray="4 4" opacity="0.6" />
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
                left: `${node.x - 130}px`,
                top: `${node.y - 65}px`,
                width: "260px",
                padding: "18px 20px",
                background: isNodeActive ? "rgba(30, 41, 59, 0.95)" : "rgba(15, 23, 42, 0.75)",
                borderRadius: "16px",
                border: `2px solid ${isNodeActive ? node.color : "rgba(255, 255, 255, 0.12)"}`,
                boxShadow: isNodeActive ? `0 0 30px ${node.color}55, 0 10px 20px rgba(0,0,0,0.4)` : "none",
                transition: "all 0.3s ease",
                zIndex: 2,
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
                <span style={{ fontSize: "17px", fontWeight: 800, color: node.color, letterSpacing: "0.5px" }}>
                  {node.label}
                </span>
                <span
                  style={{
                    width: "10px",
                    height: "10px",
                    borderRadius: "50%",
                    background: isNodeActive ? node.color : "#475569",
                    boxShadow: isNodeActive ? `0 0 10px ${node.color}` : "none",
                  }}
                />
              </div>
              <div style={{ fontSize: "12px", fontFamily: THEME.fontMono, color: THEME.textPrimary, marginBottom: "4px" }}>
                {node.key}
              </div>
              <div style={{ fontSize: "12px", color: THEME.textMuted }}>{node.desc}</div>
            </div>
          );
        })}

        {/* Moving Animated Job Packet */}
        <div
          style={{
            position: "absolute",
            left: `${packetX - 85}px`,
            top: `${packetY - 35}px`,
            width: "170px",
            padding: "10px 14px",
            background: "rgba(11, 15, 25, 0.95)",
            borderRadius: "12px",
            border: `2px solid ${activeColor}`,
            boxShadow: `0 0 25px ${activeColor}88, 0 8px 20px rgba(0,0,0,0.6)`,
            zIndex: 10,
            transform: `scale(${1 + pulse * 0.06})`,
            pointerEvents: "none",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: "11px", fontWeight: 800, color: activeColor }}>JOB #1024</span>
            <span style={{ fontSize: "10px", color: THEME.textSecondary, fontFamily: THEME.fontMono }}>attempts: 1</span>
          </div>
          <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary, marginTop: "2px" }}>
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
            background: "rgba(15, 23, 42, 0.9)",
            borderRadius: "14px",
            borderLeft: `5px solid ${activeColor}`,
            padding: "16px 24px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontSize: "16px", fontWeight: 700, color: THEME.textPrimary, marginBottom: "3px" }}>
              {currentAction}
            </div>
            <div style={{ fontSize: "13px", color: THEME.textSecondary }}>
              Executed Script: <span style={{ fontFamily: THEME.fontMono, color: activeColor, fontWeight: 600 }}>{activeScript}</span>
            </div>
          </div>
          <div
            style={{
              padding: "6px 14px",
              borderRadius: "8px",
              background: `${activeColor}22`,
              border: `1px solid ${activeColor}44`,
              color: activeColor,
              fontWeight: 800,
              fontSize: "12px",
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
