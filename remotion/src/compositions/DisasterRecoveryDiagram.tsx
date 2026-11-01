import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const DisasterRecoveryDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="High Availability & Cloud Chaos"
        title="Disaster Recovery & Chaos Resilience Architecture"
        subtitle="Zero-duplicate checkpoint resumption, Lettuce socket auto-reconnect, and StalledJobSentinel lock recovery"
        badgeText="CHAOS RESILIENCE ENGINE"
        badgeColor={THEME.amber}
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
        <div style={{ display: "flex", justifyContent: "space-between", gap: "24px", flex: 1 }}>
          {/* Pillar 1: Kubernetes Graceful Drain */}
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
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>☸️</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.blue }}>1. K8s Lifecycle</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Pod Autoscaling &amp; Drain</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.blue}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>PreStop Hook</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  sleep 10s: Unregisters pod from load balancer
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Graceful Worker Drain</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Stops pulling new jobs; allows in-flight tasks to finish
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 2: Redis Fault & Reconnect */}
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
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>⚡</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.ruby }}>2. Redis Failover</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Socket Reconnection</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.ruby }}>Lettuce Command Buffer</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Buffers non-blocking calls; auto-reconnects with exponential backoff &amp; jitter
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.amber }}>StalledJobSentinel</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Reclaims crashed worker tasks after lock TTL expires (moveStalledJobsToWait)
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 3: Zero-Duplicate Checkpointing */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.emerald}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.emerald}22`, padding: "8px", borderRadius: "8px" }}>💾</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.emerald }}>3. Checkpoint Resumption</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>CheckpointStatePlugin</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Persistent Delta Token</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.cyan, marginTop: "2px" }}>
                  oxmq:checkpoint:&lt;ns&gt;
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.purple}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Zero-Duplicate Restart</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Restarted pod reads token and resumes stream processing with zero duplicates
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            marginTop: "20px",
            background: "rgba(245, 158, 11, 0.08)",
            borderRadius: "12px",
            border: `1px solid ${THEME.amber}44`,
            padding: "14px 24px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <span style={{ fontSize: "20px" }}>🛡️</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.amber }}>Production Chaos Verified:</strong> Tested against SIGKILL pod evictions, Redis cluster failovers, and network partitioning with zero message loss and zero orphan locks.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            CheckpointStatePlugin.class
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
