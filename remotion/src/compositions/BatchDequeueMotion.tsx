import React from "react";
import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const BatchDequeueMotion: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // 300 frames total (10 seconds @ 30 fps)
  // Phase 0 (0-75): Redis waiting queue with 10 pending jobs
  // Phase 1 (75-150): Single round-trip bulk pop of 5 jobs with maxBatchBytes check
  // Phase 2 (150-225): Virtual thread executes batch; job #3 fails with item-level exception
  // Phase 3 (225-300): 4 jobs move to COMPLETED, job #3 moves to FAILED with error reason

  const popped = frame >= 75;
  const executing = frame >= 150;
  const finished = frame >= 225;

  let bannerText = "1. Queue contains accumulated backlog; OxmqBatchWorker configured with batchSize: 5";
  let activeScript = "Redis wait list queue";
  let activeColor = THEME.cyan;

  if (popped && !executing) {
    bannerText = "2. Atomic bulk pop: moveToActive.lua claims 5 jobs in 1 network roundtrip (maxBatchBytes safe)";
    activeScript = "moveToActiveBatchGroup.lua (Batch Claim)";
    activeColor = THEME.amber;
  } else if (executing && !finished) {
    bannerText = "3. Virtual Thread processes batch. Job #3 hits validation error; marked via job.setAsFailed()";
    activeScript = "processor.process(List<Job<T>> jobs)";
    activeColor = THEME.orange;
  } else if (finished) {
    bannerText = "4. Granular split: 4 jobs completed atomically, 1 job failed with error reason preserved";
    activeScript = "moveToFinished.lua (Bulk + Granular Split)";
    activeColor = THEME.emerald;
  }

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="High-Throughput Ingestion"
        title="OxMQ Batch Dequeue Pipeline & Granular Failures"
        subtitle="Bulk pop up to N jobs in a single roundtrip with maxBatchBytes memory ceiling and item-level error isolation"
        badgeText="OXMQBATCHWORKER"
        badgeColor={THEME.emerald}
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
        <div style={{ display: "flex", justifyContent: "space-between", gap: "30px", flex: 1 }}>
          {/* Redis Waiting Queue Box */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.amber}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <span style={{ fontSize: "16px", fontWeight: 800, color: THEME.amber }}>REDIS WAIT QUEUE</span>
              <span style={{ fontSize: "12px", fontFamily: THEME.fontMono, color: THEME.textMuted }}>bull:events:wait</span>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "8px", flex: 1, justifyContent: "center" }}>
              {[1, 2, 3, 4, 5].map((id) => (
                <div
                  key={id}
                  style={{
                    background: popped ? "rgba(30, 41, 59, 0.3)" : "rgba(30, 41, 59, 0.8)",
                    padding: "8px 14px",
                    borderRadius: "8px",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    borderLeft: `3px solid ${popped ? THEME.textMuted : THEME.amber}`,
                    opacity: popped ? 0.4 : 1,
                    transition: "all 0.3s ease",
                  }}
                >
                  <span style={{ fontSize: "13px", fontWeight: 600, color: THEME.textPrimary }}>event-packet #{id}</span>
                  <span style={{ fontSize: "11px", color: THEME.textMuted, fontFamily: THEME.fontMono }}>4.2 KB</span>
                </div>
              ))}
            </div>
          </div>

          {/* Atomic Bulk Pop Pipe */}
          <div style={{ width: "120px", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center" }}>
            <span style={{ fontSize: "11px", fontWeight: 700, color: THEME.cyan, textAlign: "center", marginBottom: "8px" }}>
              1 ROUNDTRIP
            </span>
            <div
              style={{
                width: "100%",
                height: "4px",
                background: `linear-gradient(90deg, ${THEME.amber}, ${THEME.cyan})`,
                boxShadow: popped ? `0 0 15px ${THEME.cyan}` : "none",
                borderRadius: "2px",
              }}
            />
            <span style={{ fontSize: "11px", color: THEME.textMuted, marginTop: "8px" }}>bulkPop(5)</span>
          </div>

          {/* Batch Worker Execution Box */}
          <div
            style={{
              flex: 1.2,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.cyan}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <span style={{ fontSize: "16px", fontWeight: 800, color: THEME.cyan }}>BATCH WORKER (LOOM THREAD)</span>
              <span
                style={{
                  fontSize: "11px",
                  fontWeight: 700,
                  padding: "3px 8px",
                  borderRadius: "8px",
                  background: finished ? `${THEME.emerald}22` : executing ? `${THEME.orange}22` : `${THEME.cyan}22`,
                  color: finished ? THEME.emerald : executing ? THEME.orange : THEME.cyan,
                }}
              >
                {finished ? "SPLIT TERMINATED" : executing ? "PROCESSING" : popped ? "ACQUIRED" : "IDLE"}
              </span>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "8px", flex: 1, justifyContent: "center" }}>
              {[
                { id: 1, name: "job-1", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
                { id: 2, name: "job-2", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
                { id: 3, name: "job-3 (Invalid Payload)", status: finished ? "FAILED" : executing ? "setAsFailed()" : "PROCESSING", color: THEME.ruby },
                { id: 4, name: "job-4", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
                { id: 5, name: "job-5", status: finished ? "COMPLETED" : "PROCESSING", color: THEME.emerald },
              ].map((j) => (
                <div
                  key={j.id}
                  style={{
                    background: popped ? "rgba(30, 41, 59, 0.85)" : "rgba(30, 41, 59, 0.2)",
                    padding: "8px 14px",
                    borderRadius: "8px",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    borderLeft: `3px solid ${popped ? j.color : "#475569"}`,
                    opacity: popped ? 1 : 0.2,
                  }}
                >
                  <span style={{ fontSize: "13px", fontWeight: 600, color: THEME.textPrimary }}>{j.name}</span>
                  <span style={{ fontSize: "11px", fontWeight: 700, color: j.color, fontFamily: THEME.fontMono }}>
                    {popped ? j.status : "waiting"}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Explanatory Ribbon */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.9)",
            borderRadius: "14px",
            borderLeft: `5px solid ${activeColor}`,
            padding: "16px 24px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: "20px",
          }}
        >
          <div>
            <div style={{ fontSize: "16px", fontWeight: 700, color: THEME.textPrimary, marginBottom: "3px" }}>
              {bannerText}
            </div>
            <div style={{ fontSize: "13px", color: THEME.textSecondary }}>
              Pipeline: <span style={{ fontFamily: THEME.fontMono, color: activeColor, fontWeight: 600 }}>{activeScript}</span>
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
            }}
          >
            MEMORY SAFE BULK POP
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
