import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const TransactionalOutboxDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Resilience Patterns"
        title="Transactional Outbox Pattern & Zero-Dual-Write Ingestion"
        subtitle="Guaranteed atomic persistence across relational databases and Redis without fragile 2PC distributed transactions"
        badgeText="TRANSACTIONAL OUTBOX"
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
        {/* Main Flow: 4 Pillars */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: "24px", flex: 1 }}>
          {/* Pillar 1: Service @Transactional */}
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
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>📦</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.blue }}>1. App Service Boundary</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>@Transactional Scope</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.blue}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Save Business Entity</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  INSERT INTO orders ...
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: THEME.textPrimary }}>Write Outbox Record</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.cyan, marginTop: "2px" }}>
                  INSERT INTO oxmq_outbox ... (status: &apos;PENDING&apos;)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "8px 10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.emerald, fontWeight: 600 }}>
                  &check; Single SQL COMMIT (Atomic)
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 2: Database Table */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.purple}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.purple}22`, padding: "8px", borderRadius: "8px" }}>🗄️</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.purple }}>2. Outbox Table</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>PostgreSQL / MySQL</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "8px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "8px 10px", borderRadius: "6px", fontFamily: THEME.fontMono, fontSize: "11px" }}>
                <div style={{ color: THEME.amber }}>id: BIGSERIAL PRIMARY KEY</div>
                <div style={{ color: THEME.textSecondary }}>queue_name: &apos;payments&apos;</div>
                <div style={{ color: THEME.textSecondary }}>payload: &apos;&#123;&quot;orderId&quot;: 101&#125;&apos;</div>
                <div style={{ color: THEME.emerald }}>status: &apos;PENDING&apos; | &apos;PUBLISHED&apos;</div>
              </div>
              <div style={{ fontSize: "11px", color: THEME.textMuted, textAlign: "center" }}>
                Indexed on (status, created_at)
              </div>
            </div>
          </div>

          {/* Pillar 3: Outbox Publisher Poller */}
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
              <div style={{ background: `${THEME.cyan}22`, padding: "8px", borderRadius: "8px" }}>⚡</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.cyan }}>3. Outbox Publisher</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Loom Poller &amp; Drainer</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>SKIP LOCKED Fetch</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "10px", color: THEME.textSecondary, marginTop: "2px" }}>
                  SELECT * ... FOR UPDATE SKIP LOCKED LIMIT 50
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.textPrimary }}>Redis Enqueue</div>
                <div style={{ fontSize: "11px", color: THEME.cyan }}>
                  OxmqQueue.add(jobName, payload)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "8px 10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.emerald, fontWeight: 600 }}>
                  &check; Mark status = &apos;PUBLISHED&apos; on ACK
                </div>
              </div>
            </div>
          </div>

          {/* Pillar 4: Redis Storage & Workers */}
          <div
            style={{
              flex: 1,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "16px",
              border: `2px solid ${THEME.ruby}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>🚀</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.ruby }}>4. Redis &amp; Workers</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Safe Asynchronous Drain</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.ruby }}>Redis LPUSH / ZADD</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Job resides safely in bull:&lt;q&gt;:wait
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.emerald }}>Worker Processing</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Worker executes with guaranteed DB consistency
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            marginTop: "20px",
            background: "rgba(6, 182, 212, 0.08)",
            borderRadius: "12px",
            border: `1px solid ${THEME.cyan}44`,
            padding: "14px 24px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <span style={{ fontSize: "20px" }}>🛡️</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.cyan }}>Zero Dual-Write Hazard:</strong> If Redis is temporarily down or the app crashes, pending jobs remain safely preserved in the relational database until connectivity is restored.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            @EnableOxmqOutbox
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
