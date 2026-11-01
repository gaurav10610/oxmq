import React from "react";

export const THEME = {
  bgDark: "#0B0F19",
  cardBg: "rgba(15, 23, 42, 0.85)",
  cardBorder: "rgba(56, 189, 248, 0.25)",
  cardBorderSubtle: "rgba(255, 255, 255, 0.1)",
  cyan: "#38BDF8",
  blue: "#0284C7",
  emerald: "#10B981",
  ruby: "#EF4444",
  orange: "#F97316",
  amber: "#F59E0B",
  purple: "#A855F7",
  indigo: "#6366F1",
  slate: "#1E293B",
  slateDark: "#0F172A",
  textPrimary: "#F8FAFC",
  textSecondary: "#94A3B8",
  textMuted: "#64748B",
  fontSans: "system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif",
  fontMono: "'JetBrains Mono', 'Fira Code', 'SF Mono', Menlo, Consolas, monospace",
};

export const BackgroundGrid: React.FC = () => (
  <div
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundImage: `
        radial-gradient(circle at 15% 20%, rgba(2, 132, 199, 0.15) 0%, transparent 45%),
        radial-gradient(circle at 85% 80%, rgba(16, 185, 129, 0.12) 0%, transparent 45%),
        radial-gradient(circle at 50% 50%, rgba(168, 85, 247, 0.08) 0%, transparent 55%),
        linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px)
      `,
      backgroundSize: "100% 100%, 100% 100%, 100% 100%, 60px 60px, 60px 60px",
      zIndex: 0,
    }}
  />
);

export const DiagramHeader: React.FC<{
  title: string;
  subtitle: string;
  category: string;
  badgeText?: string;
  badgeColor?: string;
}> = ({ title, subtitle, category, badgeText = "OXMQ ENGINE", badgeColor = THEME.cyan }) => (
  <div style={{ marginBottom: "26px", zIndex: 1, position: "relative" }}>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
        <span style={{ fontSize: "16px", fontWeight: 800, letterSpacing: "1.5px", color: badgeColor, textTransform: "uppercase" }}>
          {category}
        </span>
        <span style={{ height: "3px", width: "36px", background: badgeColor, borderRadius: "2px" }}></span>
      </div>
      <div
        style={{
          fontSize: "15px",
          fontWeight: 800,
          padding: "6px 16px",
          borderRadius: "20px",
          background: `${badgeColor}22`,
          color: badgeColor,
          border: `1.5px solid ${badgeColor}66`,
          letterSpacing: "0.5px",
        }}
      >
        ● {badgeText}
      </div>
    </div>
    <h2 style={{ fontSize: "44px", fontWeight: 800, margin: "0 0 6px 0", color: THEME.textPrimary, letterSpacing: "-0.5px" }}>
      {title}
    </h2>
    <p style={{ fontSize: "21px", fontWeight: 500, color: THEME.textSecondary, margin: 0 }}>
      {subtitle}
    </p>
  </div>
);
