import React from "react";
import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const DagWorkflowMotion: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // 360 frames total (12 seconds @ 30 fps)
  // Phase 0 (0-80): FlowProducer adds DAG tree. Parent enters WAITING_CHILDREN. Children enter WAITING.
  // Phase 1 (80-180): 3 Virtual Thread workers pick up Child 1, Child 2, Child 3 concurrently.
  // Phase 2 (180-270): Children complete one by one. Returnvalues populate parent's childrenValues Hash.
  // Phase 3 (270-360): Final child finishes. moveToWaitingChildren.lua unblocks parent -> ACTIVE!

  const c1Done = frame >= 200;
  const c2Done = frame >= 230;
  const c3Done = frame >= 260;
  const allChildrenDone = c1Done && c2Done && c3Done;
  const parentActive = frame >= 280;

  let phaseText = "1. FlowProducer submits DAG tree: Parent enters WAITING_CHILDREN, Children enter WAITING";
  let activeScript = "addParentJob.lua";
  let statusColor = THEME.cyan;

  if (frame >= 80 && frame < 180) {
    phaseText = "2. Workers claim 3 children concurrently via Virtual Threads (concurrency: 3)";
    activeScript = "moveToActive.lua";
    statusColor = THEME.amber;
  } else if (frame >= 180 && frame < 270) {
    phaseText = "3. Children finish parallel execution; results aggregate into parent's 'childrenValues' hash";
    activeScript = "moveToFinished.lua (Child Result Store)";
    statusColor = THEME.emerald;
  } else if (frame >= 270) {
    phaseText = "4. Final child finished: moveToWaitingChildren.lua automatically unblocks Parent -> ACTIVE!";
    activeScript = "moveToWaitingChildren.lua (Parent Unblock: 1)";
    statusColor = THEME.purple;
  }

  const pulse = (Math.sin(frame * 0.2) + 1) / 2;

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Orchestration & Workflow Engine"
        title="OxMQ Hierarchical DAG Workflow Resolution"
        subtitle="Multi-tier parent-child dependency trees with automatic result aggregation and cluster hash-tag safety"
        badgeText="FLOWPRODUCER DAG"
        badgeColor={THEME.cyan}
      />

      <div
        style={{
          flex: 1,
          position: "relative",
          background: THEME.cardBg,
          borderRadius: "20px",
          border: `1px solid ${THEME.cardBorderSubtle}`,
          padding: "36px",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          boxShadow: "0 20px 50px rgba(0, 0, 0, 0.6)",
          zIndex: 1,
        }}
      >
        {/* Visual Tree Layout */}
        <div style={{ position: "relative", height: "420px" }}>
          {/* SVG Connecting Branches */}
          <svg style={{ position: "absolute", top: 0, left: 0, width: "100%", height: "100%", pointerEvents: "none" }}>
            {/* Parent to Child 1 */}
            <path d="M 890 140 C 890 200, 380 200, 380 260" fill="none" stroke={c1Done ? THEME.emerald : THEME.cyan} strokeWidth="3" strokeDasharray="6 4" />
            {/* Parent to Child 2 */}
            <path d="M 890 140 C 890 200, 890 200, 890 260" fill="none" stroke={c2Done ? THEME.emerald : THEME.cyan} strokeWidth="3" strokeDasharray="6 4" />
            {/* Parent to Child 3 */}
            <path d="M 890 140 C 890 200, 1400 200, 1400 260" fill="none" stroke={c3Done ? THEME.emerald : THEME.cyan} strokeWidth="3" strokeDasharray="6 4" />
          </svg>

          {/* PARENT ROOT NODE */}
          <div
            style={{
              position: "absolute",
              top: "20px",
              left: "740px",
              width: "300px",
              padding: "20px",
              background: parentActive ? "rgba(168, 85, 247, 0.25)" : "rgba(30, 41, 59, 0.95)",
              borderRadius: "16px",
              border: `2px solid ${parentActive ? THEME.purple : THEME.cyan}`,
              boxShadow: parentActive ? `0 0 35px ${THEME.purple}66` : "0 10px 25px rgba(0,0,0,0.5)",
              zIndex: 3,
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
              <span style={{ fontSize: "12px", fontWeight: 800, color: parentActive ? THEME.purple : THEME.cyan, textTransform: "uppercase" }}>
                PARENT ROOT JOB
              </span>
              <span
                style={{
                  fontSize: "11px",
                  fontWeight: 700,
                  padding: "2px 8px",
                  borderRadius: "10px",
                  background: parentActive ? `${THEME.purple}33` : `${THEME.cyan}22`,
                  color: parentActive ? THEME.purple : THEME.cyan,
                }}
              >
                {parentActive ? "ACTIVE" : "WAITING_CHILDREN"}
              </span>
            </div>
            <div style={{ fontSize: "17px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "4px" }}>
              render-final-video
            </div>
            <div style={{ fontSize: "12px", fontFamily: THEME.fontMono, color: THEME.textMuted }}>
              dependencies: {allChildrenDone ? "0 (All resolved)" : "3 pending"}
            </div>
          </div>

          {/* 3 PARALLEL CHILD NODES */}
          {[
            { id: "c1", name: "transcode-1080p", x: "240px", done: c1Done, result: "{ videoUrl: 's3://.../1080p.mp4' }" },
            { id: "c2", name: "extract-audio-flac", x: "740px", done: c2Done, result: "{ audioUrl: 's3://.../audio.flac' }" },
            { id: "c3", name: "generate-thumbnails", x: "1240px", done: c3Done, result: "{ thumbsCount: 12 }" },
          ].map((child, idx) => {
            const isExecuting = frame >= 80 && !child.done;
            const borderColor = child.done ? THEME.emerald : isExecuting ? THEME.amber : THEME.cyan;

            return (
              <div
                key={child.id}
                style={{
                  position: "absolute",
                  top: "260px",
                  left: child.x,
                  width: "290px",
                  padding: "18px",
                  background: child.done ? "rgba(16, 185, 129, 0.15)" : "rgba(15, 23, 42, 0.9)",
                  borderRadius: "16px",
                  border: `2px solid ${borderColor}`,
                  boxShadow: child.done ? `0 0 25px ${THEME.emerald}44` : "0 10px 20px rgba(0,0,0,0.4)",
                  zIndex: 2,
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
                  <span style={{ fontSize: "11px", fontWeight: 800, color: borderColor, textTransform: "uppercase" }}>
                    CHILD TASK #{idx + 1}
                  </span>
                  <span
                    style={{
                      fontSize: "10px",
                      fontWeight: 700,
                      padding: "2px 6px",
                      borderRadius: "8px",
                      background: `${borderColor}22`,
                      color: borderColor,
                    }}
                  >
                    {child.done ? "COMPLETED" : isExecuting ? "LOOM THREAD" : "WAITING"}
                  </span>
                </div>
                <div style={{ fontSize: "15px", fontWeight: 700, color: THEME.textPrimary, marginBottom: "4px" }}>
                  {child.name}
                </div>
                <div style={{ fontSize: "11px", fontFamily: THEME.fontMono, color: child.done ? THEME.emerald : THEME.textMuted }}>
                  {child.done ? child.result : "Executing in parallel"}
                </div>
              </div>
            );
          })}
        </div>

        {/* Bottom Status Card */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.9)",
            borderRadius: "14px",
            borderLeft: `5px solid ${statusColor}`,
            padding: "16px 24px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontSize: "16px", fontWeight: 700, color: THEME.textPrimary, marginBottom: "3px" }}>
              {phaseText}
            </div>
            <div style={{ fontSize: "13px", color: THEME.textSecondary }}>
              Lua Coordination: <span style={{ fontFamily: THEME.fontMono, color: statusColor, fontWeight: 600 }}>{activeScript}</span>
            </div>
          </div>
          <div
            style={{
              padding: "6px 14px",
              borderRadius: "8px",
              background: `${statusColor}22`,
              border: `1px solid ${statusColor}44`,
              color: statusColor,
              fontWeight: 800,
              fontSize: "12px",
            }}
          >
            DAG AUTO-UNBLOCK
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
