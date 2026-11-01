import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const NamespaceHierarchyDiagram: React.FC = () => {
  const tiers = [
    { rank: "1", name: "Explicit Annotation / Method", example: "@OxmqListener(prefix = 'shipping')", color: THEME.cyan },
    { rank: "2", name: "Spring Boot Property", example: "oxmq.prefix=payments in application.yml", color: THEME.blue },
    { rank: "3", name: "Spring Application Name", example: "spring.application.name=checkout-service", color: THEME.emerald },
    { rank: "4", name: "Client Instance Scope", example: "OxmqClient.builder().prefix('auth').build()", color: THEME.purple },
    { rank: "5", name: "Environment Variable", example: "export OXMQ_PREFIX=tenant-eu-west", color: THEME.amber },
    { rank: "6", name: "JVM System Property", example: "-Doxmq.prefix=staging-alpha", color: THEME.orange },
    { rank: "7", name: "Upstream BullMQ Default", example: "\"bull\" (100% backward wire-compatible)", color: THEME.textMuted },
  ];

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Multi-Tenancy & Context Isolation"
        title="7-Tier Namespace Precedence & Cluster Hash-Tag Pinning"
        subtitle="Define Once at Service Level, Inherit Everywhere with automatic Redis Cluster CRC16 slot alignment"
        badgeText="7-TIER NAMESPACING ENGINE"
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
        <div style={{ display: "flex", justifyContent: "space-between", gap: "28px", flex: 1 }}>
          {/* Left Column: 7-Tier Precedence Ladder */}
          <div
            style={{
              flex: 1.3,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.blue}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" }}>
              <div style={{ background: `${THEME.blue}22`, padding: "6px 10px", borderRadius: "8px" }}>📶</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.blue }}>Strict 7-Tier Precedence Ladder</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Highest priority wins; automatic fallback</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "6px", flex: 1, justifyContent: "center" }}>
              {tiers.map((t) => (
                <div
                  key={t.rank}
                  style={{
                    background: "rgba(30, 41, 59, 0.7)",
                    padding: "7px 12px",
                    borderRadius: "6px",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    borderLeft: `3px solid ${t.color}`,
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "12px", fontWeight: 800, color: t.color }}>#{t.rank}</span>
                    <span style={{ fontSize: "12px", fontWeight: 600, color: THEME.textPrimary }}>{t.name}</span>
                  </div>
                  <span style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.textMuted }}>{t.example}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Right Column: Redis Cluster CRC16 Hash-Tag Pinning */}
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
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "6px 10px", borderRadius: "8px" }}>🗄️</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.ruby }}>Redis Cluster CRC16 Slot Routing</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Multi-Key Lua Cross-Slot Prevention</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", color: THEME.amber, fontWeight: 700 }}>Deterministic Hash-Tag Construction</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.textPrimary, marginTop: "4px" }}>
                  &#123;&lt;prefix&gt;:&lt;queue&gt;&#125;:wait<br />
                  &#123;&lt;prefix&gt;:&lt;queue&gt;&#125;:active<br />
                  &#123;&lt;prefix&gt;:&lt;queue&gt;&#125;:meta<br />
                  &#123;&lt;prefix&gt;:&lt;queue&gt;&#125;:events
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", color: THEME.emerald, fontWeight: 700 }}>Slot Guarantees</div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  All 11 evaluated keys resolve to the exact same cluster hash slot. Multi-key Lua scripts execute atomically with zero CROSSSLOT aborts.
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
              <strong style={{ color: THEME.cyan }}>Inherit Everywhere:</strong> Declare once via <code style={{ color: THEME.cyan }}>spring.application.name</code> and every queue, worker, batch processor, and DAG workflow inherits the prefix automatically.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            QueueConfig.prefix()
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
