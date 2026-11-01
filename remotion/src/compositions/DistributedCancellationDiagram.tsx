import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const DistributedCancellationDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Control Plane"
        title="Distributed Job Cancellation & Thread Interruption"
        subtitle="Sub-millisecond cancellation propagation via Redis Pub/Sub broadcast and Virtual Thread interruption"
        badgeText="CONTROL PLANE PROTOCOL"
        badgeColor={THEME.ruby}
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
        {/* Actors Grid */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: "24px", flex: 1 }}>
          {/* Column 1: Cancellation Initiator */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.blue}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>🛑</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.blue }}>1. Cancellation Trigger</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>API / Webhook / Admin</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>User / Admin Request</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.cyan, marginTop: "2px" }}>
                  queue.cancelJob(&quot;42&quot;, &quot;Aborted by user&quot;)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Control Frame JSON</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.amber, marginTop: "2px" }}>
                  &#123;&quot;action&quot;: &quot;CANCEL&quot;, &quot;id&quot;: &quot;42&quot;&#125;
                </div>
              </div>
            </div>
          </div>

          {/* Column 2: Redis Pub/Sub Broadcast */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "16px",
              border: `2px solid ${THEME.ruby}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>📡</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.ruby }}>2. Redis Pub/Sub Mesh</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Zero Latency Fanout</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.ruby, fontWeight: 700 }}>
                  PUBLISH bull:&lt;q&gt;:control
                </div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Dispatches cancellation message simultaneously to all subscribed worker nodes
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Fanout Latency</div>
                <div style={{ fontSize: "12px", color: THEME.emerald, fontWeight: 600 }}>
                  &lt; 1 millisecond cluster-wide broadcast
                </div>
              </div>
            </div>
          </div>

          {/* Column 3: Unaffected Worker Pods */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `1px solid ${THEME.cardBorderSubtle}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
              opacity: 0.85,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: "rgba(255,255,255,0.1)", padding: "8px", borderRadius: "8px" }}>🛡️</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.textPrimary }}>3a. Pods B, C (Idle)</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Local Map Lookup</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>activeJobs.get(&quot;42&quot;)</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted, marginTop: "2px" }}>
                  Returns null &rarr; Ignored
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>CPU Overhead</div>
                <div style={{ fontSize: "12px", color: THEME.cyan }}>
                  O(1) in-memory check (0 latency penalty)
                </div>
              </div>
            </div>
          </div>

          {/* Column 4: Target Executing Pod */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.orange}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.orange}22`, padding: "8px", borderRadius: "8px" }}>⚡</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.orange }}>3b. Target Pod (Running #42)</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Virtual Thread Interruption</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.orange}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Interrupt Virtual Thread</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "10px", color: THEME.orange, marginTop: "2px" }}>
                  thread.interrupt() + job.setCancelled(true)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.ruby}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Clean Lock Eviction</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  moveToFinished.lua &rarr; FAILED (&quot;Aborted by user&quot;)
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            marginTop: "20px",
            background: "rgba(239, 68, 68, 0.08)",
            borderRadius: "12px",
            border: `1px solid ${THEME.ruby}44`,
            padding: "14px 24px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <span style={{ fontSize: "20px" }}>⚡</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.ruby }}>Immediate I/O Unblocking:</strong> Long-running database operations, HTTP fetches, and sleep calls abort immediately upon interrupt without wasting cluster CPU cycles.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            Job.isCancelled()
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
