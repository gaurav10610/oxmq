import React from "react";
import { AbsoluteFill } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const DistributedTracingDiagram: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Distributed Observability"
        title="OpenTelemetry W3C Distributed Tracing Protocol"
        subtitle="End-to-end traceparent & tracestate context propagation across asynchronous Redis queue boundaries"
        badgeText="OPENTELEMETRY W3C NATIVE"
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
        {/* Sequence Steps */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: "20px", flex: 1 }}>
          {/* Step 1: Upstream Producer */}
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
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" }}>
              <div style={{ background: `${THEME.blue}22`, padding: "8px", borderRadius: "8px" }}>🌐</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.blue }}>1. HTTP Ingestion</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>API Gateway / Producer</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Incoming HTTP Header</div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.cyan, wordBreak: "break-all" }}>
                  traceparent: 00-4bf92f3577b34da6-00f067aa0ba902b7-01
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>TraceContext Extraction</div>
                <div style={{ fontSize: "12px", color: THEME.textPrimary }}>
                  OxmqQueue.add(&quot;task&quot;, data, opts.traceparent(...))
                </div>
              </div>
            </div>
          </div>

          {/* Step 2: Redis Wire Storage */}
          <div
            style={{
              flex: 1.2,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "16px",
              border: `2px solid ${THEME.ruby}`,
              padding: "20px",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" }}>
              <div style={{ background: `${THEME.ruby}22`, padding: "8px", borderRadius: "8px" }}>🗄️</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.ruby }}>2. Redis Job Hash</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Persistent Context Carrier</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.amber, fontWeight: 700 }}>
                  HSET bull:&lt;q&gt;:&lt;id&gt;
                </div>
                <div style={{ fontFamily: THEME.fontMono, fontSize: "11px", color: THEME.textSecondary, marginTop: "4px" }}>
                  &bull; traceparent: &quot;00-4bf92f...-01&quot;<br />
                  &bull; tracestate: &quot;rojo=1,congo=4&quot;
                </div>
              </div>
              <div style={{ background: "rgba(40, 25, 45, 0.6)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Zero Allocation Storage</div>
                <div style={{ fontSize: "12px", color: THEME.textPrimary }}>
                  Canonical key mapping ensures fast HGETALL parsing
                </div>
              </div>
            </div>
          </div>

          {/* Step 3: Consumer Loom Worker */}
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
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" }}>
              <div style={{ background: `${THEME.purple}22`, padding: "8px", borderRadius: "8px" }}>🧵</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.purple }}>3. Virtual Thread Worker</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Context Reconstruction</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Span Activation</div>
                <div style={{ fontSize: "12px", color: THEME.textPrimary }}>
                  Scope scope = tracer.withSpan(childSpan)
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Child Span Attributes</div>
                <div style={{ fontSize: "12px", color: THEME.textSecondary }}>
                  queue.name, job.id, job.attempts
                </div>
              </div>
            </div>
          </div>

          {/* Step 4: Downstream Microservices */}
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
            <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" }}>
              <div style={{ background: `${THEME.emerald}22`, padding: "8px", borderRadius: "8px" }}>🚀</div>
              <div>
                <div style={{ fontSize: "15px", fontWeight: 800, color: THEME.emerald }}>4. Downstream Egress</div>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>DB &amp; HTTP APIs</div>
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", flex: 1, justifyContent: "center" }}>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Continuous Call Graph</div>
                <div style={{ fontSize: "12px", color: THEME.textPrimary }}>
                  HTTP Client injects childSpan headers into next hop
                </div>
              </div>
              <div style={{ background: "rgba(30, 41, 59, 0.7)", padding: "10px", borderRadius: "8px" }}>
                <div style={{ fontSize: "11px", color: THEME.textMuted }}>Complete Span Trace</div>
                <div style={{ fontSize: "12px", color: THEME.emerald }}>
                  Single unified trace ID across all async hops
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
            <span style={{ fontSize: "20px" }}>🔍</span>
            <span style={{ fontSize: "13px", color: THEME.textSecondary }}>
              <strong style={{ color: THEME.purple }}>Standards Compliant:</strong> Built on official W3C Distributed Tracing specification. Compatible with OpenTelemetry SDK, Zipkin, Jaeger, Datadog, and Dynatrace out of the box.
            </span>
          </div>
          <div style={{ fontFamily: THEME.fontMono, fontSize: "12px", color: THEME.textMuted }}>
            Job.getTraceparent()
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
