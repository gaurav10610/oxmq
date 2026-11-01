import React from "react";
import { AbsoluteFill, useCurrentFrame, interpolate } from "remotion";
import { THEME, BackgroundGrid, DiagramHeader } from "../theme";

export const BackpressureMotion: React.FC = () => {
  const frame = useCurrentFrame();

  // Simulate Redis memory climbing from 120MB -> 195MB (spike) -> 150MB (throttled drain)
  let memoryMb = 120;
  if (frame < 100) {
    memoryMb = interpolate(frame, [0, 100], [120, 195], { extrapolateRight: "clamp" });
  } else if (frame < 200) {
    memoryMb = interpolate(frame, [100, 200], [195, 175], { extrapolateRight: "clamp" });
  } else {
    memoryMb = interpolate(frame, [200, 300], [175, 145], { extrapolateRight: "clamp" });
  }

  const isThrottled = memoryMb >= 180;
  const memoryPercent = Math.min(100, Math.round((memoryMb / 200) * 100));

  let statusText = "Normal Ingestion: Redis memory at healthy levels (below 180 MB ceiling)";
  let statusColor = THEME.emerald;
  let statusBadge = "HEALTHY (NORMAL)";

  if (isThrottled) {
    statusText = "Admission Gate Active: Memory >= 180 MB -> Diverting crawl jobs to moveToDelayed(skipAttempt: true)";
    statusColor = THEME.ruby;
    statusBadge = "PACING BACKPRESSURE ACTIVE";
  } else if (frame >= 200) {
    statusText = "Recovery Hysteresis: Memory drained below recovery threshold -> Resuming full ingestion";
    statusColor = THEME.cyan;
    statusBadge = "AUTOMATIC RECOVERY";
  }

  return (
    <AbsoluteFill style={{ backgroundColor: THEME.bgDark, padding: "45px 65px", fontFamily: THEME.fontSans }}>
      <BackgroundGrid />

      <DiagramHeader
        category="Memory Governance"
        title="Zero-Data-Loss Backpressure & Strict 200MB Ceiling"
        subtitle="Application-layer admission gate via JobInterceptor prevents catastrophic Redis LRU eviction and memory OOM"
        badgeText="ZERO DATA LOSS"
        badgeColor={THEME.emerald}
      />

      <div
        style={{
          flex: 1,
          position: "relative",
          background: THEME.cardBg,
          borderRadius: "22px",
          border: `1.5px solid ${THEME.cardBorderSubtle}`,
          padding: "30px",
          boxShadow: "0 20px 50px rgba(0, 0, 0, 0.6)",
          zIndex: 1,
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
        }}
      >
        {/* Main Display: Memory Gauge & Workflow Steps */}
        <div style={{ display: "flex", gap: "32px", alignItems: "center", flex: 1 }}>
          {/* Memory Gauge Card */}
          <div
            style={{
              flex: 1,
              background: "rgba(24, 15, 30, 0.85)",
              borderRadius: "18px",
              border: `2.5px solid ${isThrottled ? THEME.ruby : THEME.cyan}`,
              padding: "26px",
              display: "flex",
              flexDirection: "column",
              justifyContent: "space-between",
              height: "440px",
              boxShadow: isThrottled ? `0 0 35px ${THEME.ruby}44` : "none",
              transition: "all 0.3s ease",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: "18px", fontWeight: 800, color: THEME.textPrimary }}>REDIS MEMORY GAUGE</span>
              <span
                style={{
                  fontSize: "15px",
                  fontWeight: 800,
                  padding: "4px 12px",
                  borderRadius: "10px",
                  background: isThrottled ? `${THEME.ruby}26` : `${THEME.emerald}26`,
                  color: isThrottled ? THEME.ruby : THEME.emerald,
                }}
              >
                {isThrottled ? "PACING (DELAY)" : "NORMAL"}
              </span>
            </div>

            <div style={{ textAlign: "center", margin: "14px 0" }}>
              <div
                style={{
                  fontSize: "76px",
                  fontWeight: 900,
                  fontFamily: THEME.fontMono,
                  color: isThrottled ? THEME.ruby : THEME.cyan,
                  letterSpacing: "-2px",
                }}
              >
                {Math.round(memoryMb)} <span style={{ fontSize: "32px", color: THEME.textMuted }}>MB</span>
              </div>
              <div style={{ fontSize: "17px", color: THEME.textSecondary, fontWeight: 600 }}>
                Hard Budget: 200 MB &bull; {memoryPercent}% Used
              </div>
            </div>

            {/* Gauge Progress Bar */}
            <div style={{ width: "100%", height: "20px", background: "rgba(255,255,255,0.08)", borderRadius: "10px", overflow: "hidden", position: "relative" }}>
              <div
                style={{
                  width: `${memoryPercent}%`,
                  height: "100%",
                  background: isThrottled ? `linear-gradient(90deg, ${THEME.amber}, ${THEME.ruby})` : `linear-gradient(90deg, ${THEME.cyan}, ${THEME.emerald})`,
                  borderRadius: "10px",
                  transition: "width 0.1s linear",
                }}
              />
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "15px", color: THEME.textMuted, fontWeight: 600 }}>
              <span>0 MB</span>
              <span style={{ color: THEME.amber, fontWeight: 800 }}>180 MB (Throttle)</span>
              <span style={{ color: THEME.ruby, fontWeight: 800 }}>200 MB (Hard Ceiling)</span>
            </div>
          </div>

          {/* Workflow Architecture Card */}
          <div
            style={{
              flex: 1.3,
              display: "flex",
              flexDirection: "column",
              gap: "12px",
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
                  background: "rgba(30, 41, 59, 0.8)",
                  borderRadius: "14px",
                  borderLeft: `5px solid ${st.color}`,
                  padding: "14px 20px",
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <div>
                  <div style={{ fontSize: "18px", fontWeight: 800, color: THEME.textPrimary }}>{st.step}</div>
                  <div style={{ fontSize: "15px", color: THEME.textSecondary, marginTop: "2px" }}>{st.desc}</div>
                </div>
                <span
                  style={{
                    fontSize: "13px",
                    fontWeight: 800,
                    padding: "4px 12px",
                    borderRadius: "8px",
                    background: `${st.color}26`,
                    color: st.color,
                    letterSpacing: "0.5px",
                    whiteSpace: "nowrap",
                  }}
                >
                  {st.state}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Bottom Explanatory Ribbon */}
        <div
          style={{
            background: "rgba(15, 23, 42, 0.95)",
            borderRadius: "16px",
            borderLeft: `6px solid ${statusColor}`,
            border: `1px solid rgba(255, 255, 255, 0.12)`,
            padding: "18px 28px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontSize: "19px", fontWeight: 800, color: THEME.textPrimary, marginBottom: "4px" }}>
              {statusText}
            </div>
            <div style={{ fontSize: "16px", color: THEME.textSecondary }}>
              Admission Policy: <span style={{ color: statusColor, fontWeight: 700 }}>Skip Attempt Counting &bull; Zero Redis LRU Destructive Evictions</span>
            </div>
          </div>
          <div
            style={{
              padding: "8px 18px",
              borderRadius: "10px",
              background: `${statusColor}26`,
              border: `1.5px solid ${statusColor}55`,
              color: statusColor,
              fontWeight: 800,
              fontSize: "14px",
              letterSpacing: "0.5px",
            }}
          >
            {statusBadge}
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};
