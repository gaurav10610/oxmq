import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const DistributedRelayDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Clustering & Event Mesh"
        title="Distributed Relay & Hierarchical Topic Routing Engine"
        subtitle="Sub-millisecond pub/sub broadcast, retained messages, and ephemeral node leases with wildcard routing"
        badgeText="TOPIC RELAY ENGINE"
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
          {/* Column 1: Publisher Node Alpha */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.blue}`,
              padding: "22px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>📡</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.blue }}>Publisher (Node Alpha)</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Hierarchical Event Producer</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.blue}` }}>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Live Broadcast</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.cyan, marginTop: "2px" }}>
                  publish(&quot;orders.us-east.created&quot;, payload)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.emerald}` }}>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Retained State (TTL: 10m)</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.emerald, marginTop: "2px" }}>
                  publish(..., retainDuration: 10m)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>
                  &bull; Ephemeral Heartbeat (5s lease renewal)
                </div>
              </div>
            </div>
          </div>

          {/* Column 2: Redis Relay Infrastructure */}
          <div
            style={{
              flex: 1.2,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "16px",
              border: `2px solid ${THEME.ruby}`,
              padding: "22px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>🗄️</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.ruby }}>Redis Infrastructure</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Pub/Sub Channels &amp; Lease Keys</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.ruby, fontWeight: 700 }}>
                  bull:relay:events
                </div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Ultra-low latency pub/sub transport
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.amber, fontWeight: 700 }}>
                  bull:relay:retained:*
                </div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Stores latest event snapshot for newly joining nodes
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px 14px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.cyan, fontWeight: 700 }}>
                  bull:relay:nodes:*
                </div>
                <div style={{ fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  Ephemeral node discovery with automatic expiration
                </div>
              </div>
            </div>
          </div>

          {/* Column 3: Subscriber Nodes with Wildcard Matching */}
          <div
            style={{
              flex: 1.1,
              background: "rgba(15, 23, 42, 0.8)",
              borderRadius: "16px",
              border: `2px solid ${THEME.emerald}`,
              padding: "22px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "14px" }}>
              <div style={{ background: `${THEME.emerald}22`, padding: "8px", borderRadius: "8px" }}>🎯</div>
              <div>
                <div style={{ fontSize: "16px", fontWeight: 800, color: THEME.emerald }}>Wildcard Subscribers</div>
                <div style={{ fontSize: "12px", color: THEME.textMuted }}>Nodes Beta &amp; Gamma</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px 12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.cyan}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.cyan }}>Node Beta: Single Segment (*)</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  subscribe(&quot;orders.*.created&quot;)
                </div>
                <div style={{ fontSize: "11px", color: THEME.textMuted, marginTop: "2px" }}>Matches &apos;orders.us-east.created&apos;</div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px 12px", borderRadius: "8px", borderLeft: `3px solid ${THEME.purple}` }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: THEME.purple }}>Node Gamma: Multi Segment (&gt;)</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.textSecondary, marginTop: "2px" }}>
                  subscribe(&quot;orders.&gt;&quot;)
                </div>
                <div style={{ fontSize: "11px", color: THEME.textMuted, marginTop: "2px" }}>Matches all sub-paths under orders.*</div>
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
            <span style={{ fontSize: "20px" }}>🛡️</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.emerald }}>Zero-Duplicate Catch-up:</strong> When a subscriber connects, OxMQ delivers retained messages and active stream events through an internal deduplication barrier.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            OxmqRelay.builder()
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
