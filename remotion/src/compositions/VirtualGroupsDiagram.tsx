import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const VirtualGroupsDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Multi-Tenant Scheduling"
        title="Virtual Groups & Fair Circular Scheduling Architecture"
        subtitle="Fair multi-tenant concurrency and intra-group priority without head-of-line starvation"
        badgeText="MULTI-TENANT FAIR SHARE"
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
        {/* Top 3 Columns */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: "28px", flex: 1 }}>
          {/* Column 1: Multi-Tenant Ingestion */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.blue}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>🏢</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.blue }}>Tenant Ingestion Layer</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Tagged with groupId</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "12px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.blue}` }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>Tenant Alpha</span>
                  <span style={{ fontSize: "11px", background: "rgba(59, 130, 246, 0.2)", color: THEME.blue, padding: "2px 8px", borderRadius: "4px" }}>groupId: &apos;org-alpha&apos;</span>
                </div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary, marginTop: "4px" }}>Burst traffic: 10,000 jobs enqueued</div>
              </div>

              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>Tenant Beta</span>
                  <span style={{ fontSize: "11px", background: "rgba(16, 185, 129, 0.2)", color: THEME.emerald, padding: "2px 8px", borderRadius: "4px" }}>groupId: &apos;org-beta&apos;</span>
                </div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary, marginTop: "4px" }}>Steady stream: 50 jobs/sec (VIP SLA)</div>
              </div>

              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.purple}` }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>Tenant Gamma</span>
                  <span style={{ fontSize: "11px", background: "rgba(168, 85, 247, 0.2)", color: THEME.purple, padding: "2px 8px", borderRadius: "4px" }}>groupId: &apos;org-gamma&apos;</span>
                </div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary, marginTop: "4px" }}>Occasional high-priority batch jobs</div>
              </div>
            </div>
          </div>

          {/* Column 2: Redis Virtual Groups Engine */}
          <div
            style={{
              flex: 1.3,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "16px",
              border: `2px solid ${THEME.ruby}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>🗄️</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.ruby }}>Redis Group Topology &amp; Lua Arbiter</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Circular Round-Robin Pointer</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontFamily: THEME.fontMono, fontSize: "13px", color: THEME.textPrimary, fontWeight: 700 }}>bull:&lt;q&gt;:groups</span>
                  <span style={{ fontSize: "11px", color: THEME.amber }}>Active Group Circular Ring</span>
                </div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary, marginTop: "4px" }}>[&apos;org-alpha&apos; &rarr; &apos;org-beta&apos; &rarr; &apos;org-gamma&apos;]</div>
              </div>

              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "13px", color: THEME.textPrimary, fontWeight: 700 }}>bull:&lt;q&gt;:group:&lt;groupId&gt;:wait</div>
                <div style={{ fontSize: "12px", color: THEME.cyan, marginTop: "2px" }}>Isolated per-tenant FIFO queues / priority buckets</div>
              </div>

              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "13px", color: THEME.textPrimary, fontWeight: 700 }}>Group Concurrency Tracker</div>
                <div style={{ fontSize: "12px", color: THEME.emerald, marginTop: "2px" }}>Tracks in-flight workers per group (maxConcurrency enforcement)</div>
              </div>
            </div>
          </div>

          {/* Column 3: Virtual Thread Workers */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.emerald}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
              <div style={{ background: `${THEME.emerald}22`, padding: "8px", borderRadius: "8px" }}>⚡</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.emerald }}>Virtual Thread Execution</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Fair Share Dispatcher</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>Zero Starvation</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Tenant Alpha&apos;s 10,000 jobs cannot monopolize worker pool</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>Per-Group Rate Limits</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Individual quotas evaluated during Lua dequeue pass</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>Auto Group Cleanup</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Empty tenant groups evicted from ring dynamically (0 leak)</div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            marginTop: "24px",
            background: "rgba(16, 185, 129, 0.08)",
            borderRadius: "12px",
            border: `1px solid ${THEME.emerald}44`,
            padding: "16px 24px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "14px" }}>
            <span style={{ fontSize: "20px" }}>🛡️</span>
            <div>
              <span style={{ fontSize: "14px", fontWeight: 700, color: THEME.emerald }}>Multi-Tenant Guarantee: </span>
              <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
                Every tenant receives equal round-robin scheduling opportunities. Single-tenant spikes are isolated to their designated group queue.
              </span>
            </div>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            QueueOption: .groupKey(&quot;orgId&quot;)
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
