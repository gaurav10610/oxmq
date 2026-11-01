import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const RequestReplyDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Messaging Patterns"
        title="Request-Reply &amp; Asynchronous Event Loop Architecture"
        subtitle="Race-free result retrieval via atomic isFinished check and non-blocking Redis Stream notifications"
        badgeText="REQUEST-REPLY PATTERN"
        badgeColor={THEME.purple}
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
        <div style={{ display: "flex", justifyContent: "space-between", gap: "24px", flex: 1 }}>
          {/* Pillar 1: Caller */}
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
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>🌐</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.blue }}>1. Client / Controller</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Async Non-Blocking Call</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Job Submission</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.cyan, marginTop: "2px" }}>
                  queue.add(&quot;calc&quot;, payload)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Non-Blocking Wait</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.purple, marginTop: "2px" }}>
                  waitUntilFinishedAsync(id, events, 20s)
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 2: Atomic Lua Check */}
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
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.ruby }}>2. Atomic Fast Path</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>isFinished-3.lua Check</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", color: THEME.emerald, fontWeight: 700 }}>Already Completed?</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Returns returnvalue immediately (O(1) zero-wait)
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", color: THEME.amber, fontWeight: 700 }}>Still In Flight?</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Registers one-shot handler on Redis Stream listener
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 3: QueueEvents Stream */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.cyan}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.cyan}22`, padding: "8px", borderRadius: "8px" }}>📡</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.cyan }}>3. QueueEvents Stream</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>bull:&lt;q&gt;:events</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.cyan }}>Event Multiplexing</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Single XREAD stream consumer delivers events to all waiting futures
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.emerald }}>Automatic Deregistration</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Handlers auto-detach on completion or timeout (zero memory leak)
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 4: Worker & Completion */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.emerald}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.emerald}22`, padding: "8px", borderRadius: "8px" }}>🧵</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.emerald }}>4. Virtual Worker</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Execution &amp; XADD</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.emerald }}>moveToFinished</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Worker finishes job and atomically emits &apos;completed&apos; event
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Future Completed</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Future.complete(output) resolves REST response
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            marginTop: "20px",
            background: "rgba(168, 85, 247, 0.08)",
            borderRadius: "12px",
            border: `1px solid ${THEME.purple}44`,
            padding: "14px 24px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <span style={{ fontSize: "20px" }}>🛡️</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.purple }}>Zero Race Condition:</strong> Combining the initial Lua check with stream listening guarantees that whether the job finishes before or after listening, the caller always gets the result.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            queue.waitUntilFinishedAsync()
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
