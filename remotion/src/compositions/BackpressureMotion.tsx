import React from "react";
import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const BackpressureMotion: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // 300 frames total (10 seconds @ 30 fps)
  // Phase 0 (0-80): High throughput ingestion, memory climbing (100MB -> 180MB)
  // Phase 1 (80-160): Memory hits 190MB threshold; JobInterceptor admission gate activates
  // Phase 2 (160-240): Ingestion diverted to moveToDelayed(skipAttempt: true); memory ceiling held
  // Phase 3 (240-300): Workers process active queue; memory drops safely back to 140MB

  const memoryMb = Math.round(
    interpolate(
      frame,
      [0, 80, 160, 240, 300],
      [90, 182, 194, 165, 130],
      { extrapolateRight: "clamp" }
    )
  );

  const isThrottled = frame >= 80 && frame < 220;
  const isRecovered = frame >= 220;

  const statusColor = isThrottled ? THEME.ruby : isRecovered ? THEME.emerald : THEME.cyan;
  const statusBadge = isThrottled ? "ADMISSION GATE: ACTIVE THROTTLE" : isRecovered ? "DRAINING NORMALLY" : "HEALTHY INGESTION";

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "50px 70px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Memory Protection & Admission Control"
        title="Strict 200MB Redis Memory Ceiling & Zero-Data-Loss Backpressure"
        subtitle="Application-layer admission gate using moveToDelayed(skipAttempt: true) prevents catastrophic Redis LRU evictions"
        badgeText="ZERO-DATA-LOSS BACKPRESSURE"
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
        <div style={{ display: "flex", gap: "36px", flex: 1, alignItems: "center" }}>
          {/* Gauge & Metrics Card */}
          <div
            style={{
              flex: 1,
              background: "rgba(15, 23, 42, 0.9)",
              borderRadius: "16px",
              border: `2px solid ${statusColor}`,
              padding: "26px",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              boxShadow: `0 0 30px ${statusColor}33`,
            }}
          >
            <span style={{ fontSize: "14px", fontWeight: 700, color: THEME.textSecondary, textTransform: "uppercase", marginBottom: "8px" }}>
              REDIS CLUSTER MEMORY (200 MB CEILING)
            </span>

            <div style={{ fontSize: "64px", fontWeight: 900, color: statusColor, fontFamily: THEME.fontMono, margin: "10px 0" }}>
              {memoryMb} <span style={{ fontSize: "28px", color: THEME.textMuted }}>MB</span>
            </div>

            {/* Gauge Progress Bar */}
            <div style={{ width: "100%", height: "16px", background: "#334155", borderRadius: "8px", overflow: "hidden", margin: "14px 0" }}>
              <div
                style={{
                  width: `${(memoryMb / 200) * 100}%`,
                  height: "100%",
                  background: `linear-gradient(90deg, ${THEME.emerald}, ${memoryMb > 175 ? THEME.ruby : THEME.amber})`,
                  boxShadow: `0 0 10px ${statusColor}`,
                  transition: "width 0.2s ease",
                }}
              />
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", width: "100%", fontSize: "12px", color: THEME.textMuted }}>
              <span>0 MB</span>
              <span style={{ color: THEME.amber, fontWeight: 700 }}>180 MB (Throttle)</span>
              <span style={{ color: THEME.ruby, fontWeight: 700 }}>200 MB (Hard Ceiling)</span>
            </div>
          </div>

          {/* Workflow Architecture Card */}
          <div
            style={{
              flex: 1.2,
              display: "flex",
              flexDirection: "column",
              gap: "14px",
            }}
          >
            {[
              {
                step: "1. Incoming Traffic Wave",
                desc: "High volume of events entering from HTTP/SaaS connectors",
                state: "3,500 msg/sec",
                color: THEME.cyan,
              },
              {
                step: "2. Memory Sentinel Gate",
                desc: "JobInterceptor evaluates used_memory via Redis INFO",
                state: memoryMb >= 180 ? "TRIGGERED (90% Ceiling)" : "PASS",
                color: memoryMb >= 180 ? THEME.ruby : THEME.emerald,
              },
              {
                step: "3. Graceful Throttle Diversion",
                desc: "Diverts incoming jobs to moveToDelayed(skipAttempt: true)",
                state: isThrottled ? "ACTIVE DIVERSION" : "STANDBY",
                color: isThrottled ? THEME.ruby : THEME.textMuted,
              },
              {
                step: "4. Zero-Loss Data Preservation",
                desc: "No jobs dropped, no Redis LRU eviction, resume on drain",
                state: "100% DATA PRESERVED",
                color: THEME.emerald,
              },
            ].map((st, i) => (
              <div
                key={i}
                style={{
                  background: "rgba(30, 41, 59, 0.7)",
                  borderRadius: "12px",
                  borderLeft: `4px solid ${st.color}`,
                  padding: "12px 18px",
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <div>
                  <div style={{ fontSize: "15px", fontWeight: 700, color: THEME.textPrimary }}>{st.step}</div>
                  <div style={{ fontSize: "12px", color: THEME.textSecondary }}>{st.desc}</div>
                </div>
                <span
                  style={{
                    fontSize: "11px",
                    fontWeight: 800,
                    padding: "4px 10px",
                    borderRadius: "6px",
                    background: `${st.color}22`,
                    color: st.color,
                    fontFamily: THEME.fontMono,
                  }}
                >
                  {st.state}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Bottom Banner */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.9)",
            borderRadius: "14px",
            borderLeft: `5px solid ${statusColor}`,
            padding: "16px 24px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: "20px",
          }}
        >
          <div>
            <div style={{ fontSize: "16px", fontWeight: 700, color: THEME.textPrimary, marginBottom: "3px" }}>
              {isThrottled
                ? "Memory threshold exceeded (190MB/200MB): Admission gate smoothly paused intake"
                : isRecovered
                ? "Virtual Thread workers drained backlog: Memory normalized, intake auto-resumed"
                : "Queue operating within safe boundaries (nominal memory intake)"}
            </div>
            <div style={{ fontSize: "13px", color: THEME.textSecondary }}>
              Mechanism: <span style={{ fontFamily: THEME.fontMono, color: statusColor, fontWeight: 600 }}>JobInterceptor + moveToDelayed(skipAttempt: true)</span>
            </div>
          </div>
          <div
            style={{
              padding: "6px 14px",
              borderRadius: "8px",
              background: `${statusColor}22`,
              border: `1px solid ${statusColor}44`,
              color: statusColor,
              fontWeight: 800,
              fontSize: "12px",
            }}
          >
            {statusBadge}
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
