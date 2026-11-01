import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const MemoryOptimizationDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Resource Efficiency"
        title="Two-Tier Memory Optimization Architecture"
        subtitle="JVM zero-allocation hot paths and Redis compact storage structures engineered for 50,000+ jobs/sec"
        badgeText="ZERO-ALLOCATION OPTIMIZED"
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
        <div style={{ display: "flex", justifyContent: "space-between", gap: "28px", flex: 1 }}>
          {/* Tier 1: JVM Consumer */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.blue}`,
              padding: "22px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>☕</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.blue }}>Tier 1: JVM Consumer &amp; Worker</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Zero GC Storm Architecture</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px 14px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Loom Virtual Threads</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  ~1 KB heap footprint per task (vs 1 MB OS platform thread stack)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px 14px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Canonical Hash Key Interning</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Standard BullMQ keys point to static final references (0 transient Strings)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px 14px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Lazy Child DAG Resolution &amp; 8KB Trace Cap</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  0 overhead for standalone jobs; recursive exceptions capped at 8 KB
                </div>
              </div>
            </div>
          </div>

          {/* Tier 2: Redis Storage */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "16px",
              border: `2px solid ${THEME.ruby}`,
              padding: "22px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>⚡</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.ruby }}>Tier 2: Redis Storage Engine</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Bounded Footprint &amp; Automatic Compaction</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px", borderLeft: `3px solid ${THEME.ruby}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Stream Trimming (XADD MAXLEN ~ N)</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Events stream strictly bounded to prevent unbounded stream bloat
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px", borderLeft: `3px solid ${THEME.amber}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Default Job Retention Policies</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Queue-wide removeOnComplete &amp; removeOnFail age/count thresholds
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Lettuce Socket Multiplexing</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  All heartbeat watchdogs multiplexed over a single TCP connection
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            marginTop: "20px",
            background: "rgba(16, 185, 129, 0.08)",
            borderRadius: "12px",
            border: `1px solid ${THEME.emerald}44`,
            padding: "14px 24px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <span style={{ fontSize: "20px" }}>🚀</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.emerald }}>50,000+ Jobs/Sec Benchmarked:</strong> Sustained throughput with microsecond GC pause times and zero memory degradation across multi-day runs.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            QueueConfig.defaultJobOptions()
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
