import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const LockWatchdogDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Watchdog & Reliability Protocol"
        title="Lock Extender & Stalled Job Sentinel Architecture"
        subtitle="Automatic heartbeat renewal prevents false-stalls while a detached sentinel recovers dead workers"
        badgeText="HEARTBEAT WATCHDOG"
        badgeColor={THEME.orange}
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
        {/* 3 Actor Columns */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: "30px", flex: 1 }}>
          {/* Actor 1: Active Worker */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.cyan}`,
              padding: "24px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
              <div style={{ background: `${THEME.cyan}22`, padding: "8px", borderRadius: "8px" }}>🧵</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.cyan }}>Worker (Virtual Thread)</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>OxmqWorker &amp; LockExtender</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>1. Claims Job Lock</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>lockDurationMs: 30,000 ms</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>2. LockExtender Heartbeat</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Fires every lockDuration / 2 (15s)</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>3. Extends Lock PEXPIRE</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>extendLock.lua resets TTL to 30s</div>
              </div>
            </div>
          </div>

          {/* Actor 2: Redis Storage */}
          <div
            style={{
              flex: 1.1,
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
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.ruby }}>Redis Storage Engine</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Keyspace &amp; Lock Keys</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "13px", color: THEME.textPrimary, fontWeight: 700 }}>bull:&lt;q&gt;:&lt;id&gt;:lock</div>
                <div style={{ fontSize: "12px", color: THEME.orange }}>Worker Token (PEXPIRE: 30000)</div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "13px", color: THEME.textPrimary, fontWeight: 700 }}>bull:&lt;q&gt;:stalled-check</div>
                <div style={{ fontSize: "12px", color: THEME.amber }}>Debounce timestamp guard (30s)</div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "13px", color: THEME.textPrimary, fontWeight: 700 }}>bull:&lt;q&gt;:active</div>
                <div style={{ fontSize: "12px", color: THEME.ruby }}>Currently processing job set</div>
              </div>
            </div>
          </div>

          {/* Actor 3: Stalled Sentinel */}
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
              <div style={{ background: `${THEME.emerald}22`, padding: "8px", borderRadius: "8px" }}>🛡️</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.emerald }}>Stalled Sentinel</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>StalledJobSentinel Background Loop</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>1. Periodic Scan</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Runs moveStalledJobs.lua every 30s</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>2. Lock Expiry Verification</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Checks if lock key disappeared (worker died)</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "14px", fontWeight: 700, color: THEME.textPrimary }}>3. Recovers or Discards</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>Moves to wait (if attempts left) or failed</div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Protocol Note */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.9)",
            borderRadius: "14px",
            borderLeft: `5px solid ${THEME.orange}`,
            padding: "16px 24px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: "20px",
          }}
        >
          <div>
            <div style={{ fontSize: "16px", fontWeight: 700, color: THEME.textPrimary, marginBottom: "3px" }}>
              Dual-Safety Guarantee: Healthy long-running jobs are never killed; crashed pods are recovered in seconds
            </div>
            <div style={{ fontSize: "13px", color: THEME.textSecondary }}>
              Heartbeat: <span style={{ fontFamily: THEME.fontMono, color: THEME.cyan }}>LockExtender (ScheduledExecutorService)</span> · Recovery: <span style={{ fontFamily: THEME.fontMono, color: THEME.emerald }}>moveStalledJobs.lua (maxStalledCount: 2)</span>
            </div>
          </div>
          <div
            style={{
              padding: "6px 14px",
              borderRadius: "8px",
              background: `${THEME.orange}22`,
              border: `1px solid ${THEME.orange}44`,
              color: THEME.orange,
              fontWeight: 800,
              fontSize: "12px",
            }}
          >
            FAULT TOLERANT
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
