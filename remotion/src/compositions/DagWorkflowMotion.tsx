import React from "react";
import { AbsoluteFill, useCurrentFrame, interpolate } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const DagWorkflowMotion: React.FC = () => {
  const frame = useCurrentFrame();

  // Child completion times
  const c1Done = frame >= 120;
  const c2Done = frame >= 170;
  const c3Done = frame >= 220;
  const allChildrenDone = c1Done && c2Done && c3Done;
  const parentActive = frame >= 250;

  let currentPhase = "1. Producer submits DAG tree (parent + 3 children) atomically via addParentJob.lua";
  let activeScript = "addParentJob-6.lua & addChildren-4.lua";
  let activeColor = THEME.cyan;

  if (frame >= 80 && !allChildrenDone) {
    currentPhase = "2. Children execute concurrently on Virtual Threads; Parent yields in WAITING_CHILDREN";
    activeScript = "moveToActive.lua (Parallel Workers)";
    activeColor = THEME.amber;
  } else if (allChildrenDone && !parentActive) {
    currentPhase = "3. Last child completes! Redis stores outputs in parent.childrenValues and decrements dependencies to 0";
    activeScript = "moveToFinished-14.lua (Auto Parent Promotion)";
    activeColor = THEME.emerald;
  } else if (parentActive) {
    currentPhase = "4. Parent promoted to WAITING -> claimed by worker; aggregates all child results";
    activeScript = "moveToActive.lua & parentJob.getChildrenValues()";
    activeColor = THEME.purple;
  }

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "45px 65px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Orchestration & Workflows"
        title="Parent-Child DAG Workflow Engine (FlowProducer)"
        subtitle="Automatic multi-tree dependency resolution and return-value aggregation with zero external workflow engines"
        badgeText="ATOMIC DAG ENGINE"
        badgeColor={THEME.purple}
      />

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
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
        }}
      >
        {/* Canvas Area */}
        <div style={{ position: "relative", flex: 1 }}>
          {/* Connecting SVG Arrows from Children to Parent */}
          <svg style={{ position: "absolute", top: 0, left: 0, width: "100%", height: "100%", pointerEvents: "none" }}>
            {/* Child 1 -> Parent */}
            <path
              d="M 350 290 C 350 180, 800 180, 820 135"
              fill="none"
              stroke={c1Done ? THEME.emerald : THEME.cyan}
              strokeWidth="3.5"
              strokeDasharray={c1Done ? "none" : "6 4"}
              opacity="0.8"
            />
            {/* Child 2 -> Parent */}
            <path
              d="M 890 290 L 890 135"
              fill="none"
              stroke={c2Done ? THEME.emerald : THEME.cyan}
              strokeWidth="3.5"
              strokeDasharray={c2Done ? "none" : "6 4"}
              opacity="0.8"
            />
            {/* Child 3 -> Parent */}
            <path
              d="M 1430 290 C 1430 180, 980 180, 960 135"
              fill="none"
              stroke={c3Done ? THEME.emerald : THEME.cyan}
              strokeWidth="3.5"
              strokeDasharray={c3Done ? "none" : "6 4"}
              opacity="0.8"
            />
          </svg>

          {/* PARENT NODE */}
          <div
            style={{
              position: "absolute",
              top: "15px",
              left: "680px",
              width: "420px",
              padding: "20px 24px",
              background: parentActive ? "rgba(168, 85, 247, 0.25)" : "rgba(30, 41, 59, 0.95)",
              borderRadius: "18px",
              border: `2.5px solid ${parentActive ? THEME.purple : THEME.cyan}`,
              boxShadow: parentActive ? `0 0 35px ${THEME.purple}66` : "0 10px 25px rgba(0,0,0,0.5)",
              zIndex: 3,
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
              <span style={{ fontSize: "15px", fontWeight: 800, color: parentActive ? THEME.purple : THEME.cyan, textTransform: "uppercase" }}>
                PARENT ROOT JOB
              </span>
              <span
                style={{
                  fontSize: "14px",
                  fontWeight: 800,
                  padding: "4px 12px",
                  borderRadius: "12px",
                  background: parentActive ? `${THEME.purple}33` : `${THEME.cyan}22`,
                  color: parentActive ? THEME.purple : THEME.cyan,
                }}
              >
                {parentActive ? "ACTIVE" : "WAITING_CHILDREN"}
              </span>
            </div>
            <div style={{ fontSize: "22px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "6px" }}>
              render-final-video
            </div>
            <div style={{ fontSize: "16px", fontFamily: THEME.fontMono, color: THEME.textMuted, fontWeight: 600 }}>
              dependencies: {allChildrenDone ? "0 (All resolved)" : "3 pending"}
            </div>
          </div>

          {/* 3 PARALLEL CHILD NODES */}
          {[
            { id: "c1", name: "transcode-1080p", x: "160px", done: c1Done, result: "{ videoUrl: 's3://.../1080p.mp4' }" },
            { id: "c2", name: "extract-audio-flac", x: "700px", done: c2Done, result: "{ audioUrl: 's3://.../audio.flac' }" },
            { id: "c3", name: "generate-thumbnails", x: "1240px", done: c3Done, result: "{ thumbsCount: 12 }" },
          ].map((child, idx) => {
            const isExecuting = frame >= 80 && !child.done;
            const borderColor = child.done ? THEME.emerald : isExecuting ? THEME.amber : THEME.cyan;

            return (
              <div
                key={child.id}
                style={{
                  position: "absolute",
                  top: "290px",
                  left: child.x,
                  width: "380px",
                  padding: "20px 24px",
                  background: child.done ? "rgba(16, 185, 129, 0.18)" : "rgba(15, 23, 42, 0.92)",
                  borderRadius: "18px",
                  border: `2.5px solid ${borderColor}`,
                  boxShadow: child.done ? `0 0 30px ${THEME.emerald}55` : "0 10px 20px rgba(0,0,0,0.4)",
                  zIndex: 2,
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                  <span style={{ fontSize: "14px", fontWeight: 800, color: borderColor, textTransform: "uppercase" }}>
                    CHILD TASK #{idx + 1}
                  </span>
                  <span
                    style={{
                      fontSize: "13px",
                      fontWeight: 800,
                      padding: "3px 10px",
                      borderRadius: "10px",
                      background: `${borderColor}26`,
                      color: borderColor,
                    }}
                  >
                    {child.done ? "COMPLETED" : isExecuting ? "LOOM THREAD" : "WAITING"}
                  </span>
                </div>
                <div style={{ fontSize: "19px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "6px" }}>
                  {child.name}
                </div>
                <div style={{ fontSize: "14px", fontFamily: THEME.fontMono, color: child.done ? THEME.emerald : THEME.textMuted, fontWeight: 600 }}>
                  {child.done ? child.result : "Executing in parallel"}
                </div>
              </div>
            );
          })}
        </div>

        {/* Bottom Status Card */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.95)",
            borderRadius: "16px",
            borderLeft: `6px solid ${activeColor}`,
            border: `1px solid rgba(255, 255, 255, 0.12)`,
            padding: "18px 28px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontSize: "19px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "4px" }}>
              {currentPhase}
            </div>
            <div style={{ fontSize: "16px", color: THEME.textSecondary }}>
              Pipeline Lua Script: <span style={{ fontFamily: THEME.fontMono, color: activeColor, fontWeight: 700 }}>{activeScript}</span>
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
            PARALLEL LEAF DRAIN
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
