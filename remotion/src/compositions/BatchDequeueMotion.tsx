import React from "react";
import { AbsoluteFill, useCurrentFrame, interpolate } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const BatchDequeueMotion: React.FC = () => {
  const frame = useCurrentFrame();

  const popped = frame >= 60;
  const executing = frame >= 120 && frame < 220;
  const finished = frame >= 220;

  let stepText = "1. Waiting jobs accumulate in Redis bull:<queue>:wait (maxBatchSize: 5, maxBatchBytes: 1MB)";
  let stepColor = THEME.cyan;
  let statusBadge = "BUFFER ACCUMULATION";

  if (frame >= 60 && frame < 120) {
    stepText = "2. Worker executes single bulk pop roundtrip via Lua script -> Atomically moves 5 jobs to active";
    stepColor = THEME.cyan;
    statusBadge = "ATOMIC BULK POP";
  } else if (frame >= 120 && frame < 220) {
    stepText = "3. Virtual Thread processes batch. Job #3 fails validation -> worker calls job3.setAsFailed(err)";
    stepColor = THEME.orange;
    statusBadge = "GRANULAR PARTIAL FAILURE";
  } else if (frame >= 220) {
    stepText = "4. Batch completes! Jobs 1,2,4,5 saved as COMPLETED; Job 3 saved as FAILED in a single Lua pass";
    stepColor = THEME.emerald;
    statusBadge = "SPLIT ATOMIC COMPLETION";
  }

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "45px 65px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="High-Throughput Ingestion"
        title="Atomic Batch Dequeue & Granular Partial Failures"
        subtitle="Bulk pop up to N jobs in 1 Redis roundtrip with byte-level memory safety and item-level partial failure handling"
        badgeText="OXMQ BATCH WORKER"
        badgeColor={THEME.orange}
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
        {/* Main Columns */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: "28px", flex: 1, alignItems: "center" }}>
          {/* Redis Queue Column */}
          <div
            style={{
              flex: 1,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "18px",
              border: `2.5px solid ${THEME.ruby}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
              height: "440px",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <span style={{ fontSize: "20px", fontWeight: 800, color: THEME.ruby }}>bull:&lt;queue&gt;:wait</span>
              <span style={{ fontSize: "14px", fontWeight: 800, padding: "4px 10px", borderRadius: "10px", background: "rgba(239, 68, 68, 0.2)", color: THEME.ruby }}>
                {popped ? "0 JOBS" : "5 ACCUMULATED"}
              </span>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              {[101, 102, 103, 104, 105].map((id) => (
                <div
                  key={id}
                  style={{
                    background: "rgba(40, 25, 45, 0.7)",
                    borderRadius: "10px",
                    padding: "10px 16px",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    borderLeft: `4px solid ${THEME.amber}`,
                    opacity: popped ? 0.35 : 1,
                    transition: "all 0.3s ease",
                  }}
                >
                  <span style={{ fontSize: "17px", fontWeight: 700, color: THEME.textPrimary }}>event-packet #{id}</span>
                  <span style={{ fontSize: "14px", color: THEME.textMuted, fontFamily: THEME.fontMono, fontWeight: 600 }}>4.2 KB</span>
                </div>
              ))}
            </div>
          </div>

          {/* Atomic Bulk Pop Pipe */}
          <div style={{ width: "160px", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center" }}>
            <span style={{ fontSize: "14px", fontWeight: 800, color: THEME.cyan, textAlign: "center", marginBottom: "8px", letterSpacing: "0.5px" }}>
              1 ROUNDTRIP
            </span>
            <div
              style={{
                width: "100%",
                height: "5px",
                background: `linear-gradient(90deg, ${THEME.amber}, ${THEME.cyan})`,
                boxShadow: popped ? `0 0 20px ${THEME.cyan}` : "none",
                borderRadius: "3px",
              }}
            />
            <span style={{ fontSize: "14px", color: THEME.textSecondary, marginTop: "8px", fontFamily: THEME.fontMono, fontWeight: 700 }}>bulkPop(5)</span>
          </div>

          {/* Batch Worker Execution Box */}
          <div
            style={{
              flex: 1.3,
              background: "rgba(15, 23, 42, 0.85)",
              borderRadius: "18px",
              border: `2.5px solid ${THEME.cyan}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
              height: "440px",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <span style={{ fontSize: "20px", fontWeight: 800, color: THEME.cyan }}>BATCH WORKER (LOOM THREAD)</span>
              <span
                style={{
                  fontSize: "14px",
                  fontWeight: 800,
                  padding: "4px 12px",
                  borderRadius: "10px",
                  background: finished ? `${THEME.emerald}26` : executing ? `${THEME.orange}26` : `${THEME.cyan}26`,
                  color: finished ? THEME.emerald : executing ? THEME.orange : THEME.cyan,
                }}
              >
                {finished ? "SPLIT TERMINATED" : executing ? "PROCESSING" : popped ? "ACQUIRED" : "IDLE"}
              </span>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              {[
                { id: 1, name: "job-1", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
                { id: 2, name: "job-2", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
                { id: 3, name: "job-3 (Invalid Payload)", status: finished ? "FAILED" : executing ? "setAsFailed()" : "PROCESSING", color: THEME.ruby },
                { id: 4, name: "job-4", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
                { id: 5, name: "job-5", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
              ].map((item) => (
                <div
                  key={item.id}
                  style={{
                    background: "rgba(30, 41, 59, 0.8)",
                    borderRadius: "10px",
                    padding: "10px 16px",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    borderLeft: `4px solid ${popped ? item.color : THEME.textMuted}`,
                  }}
                >
                  <span style={{ fontSize: "16px", fontWeight: 700, color: THEME.textPrimary }}>{item.name}</span>
                  <span
                    style={{
                      fontSize: "13px",
                      fontWeight: 800,
                      fontFamily: THEME.fontMono,
                      color: popped ? item.color : THEME.textMuted,
                    }}
                  >
                    {popped ? item.status : "WAITING"}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Bottom Explanatory Ribbon */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.95)",
            borderRadius: "16px",
            borderLeft: `6px solid ${stepColor}`,
            border: `1px solid rgba(255, 255, 255, 0.12)`,
            padding: "18px 28px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontSize: "19px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "4px" }}>
              {stepText}
            </div>
            <div style={{ fontSize: "16px", color: THEME.textSecondary }}>
              Pipeline Advantage: <span style={{ color: stepColor, fontWeight: 700 }}>5x Fewer Network Roundtrips &bull; Zero Head-of-Line Blocking</span>
            </div>
          </div>
          <div
            style={{
              padding: "8px 18px",
              borderRadius: "10px",
              background: `${stepColor}26`,
              border: `1.5px solid ${stepColor}55`,
              color: stepColor,
              fontWeight: 800,
              fontSize: "14px",
              letterSpacing: "0.5px",
            }}
          >
            {statusBadge}
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
