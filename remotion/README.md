# OxMQ Remotion Motion Graphics & Video Engine

This directory contains the automated **Remotion** video generation pipeline for **OxMQ**, producing programmatic 1080p motion architecture diagrams and conceptual videos for the documentation and README.

---

## 🚀 Quickstart

### 1. Install Dependencies
```bash
cd remotion
npm install
```

### 2. Launch Interactive Remotion Studio (Browser Preview & Scrubber)
```bash
npm start
```
This opens the Remotion Player UI in your browser at `http://localhost:3000`, allowing frame-by-frame scrubbing, live hot-reloading as you edit code, and audio/visual inspection.

### 3. Render 1080p MP4 Video
```bash
npm run build
```
Renders the full 32-second 1080p video to `remotion/out/oxmq-architecture.mp4`.

### 4. Render Still Snapshots of Scenes
```bash
# Scene 1: Hero Intro (Frame 60)
npx remotion still src/index.ts OxmqArchitecture out/frame-60.png --frame=60

# Scene 2: 3-Tier System Topology (Frame 220)
npx remotion still src/index.ts OxmqArchitecture out/frame-220.png --frame=220

# Scene 3: Atomic Job Lifecycle (Frame 550)
npx remotion still src/index.ts OxmqArchitecture out/frame-550.png --frame=550

# Scene 4: Enterprise Capabilities (Frame 760)
npx remotion still src/index.ts OxmqArchitecture out/frame-760.png --frame=760

# Scene 5: Code Simplicity & Outro (Frame 900)
npx remotion still src/index.ts OxmqArchitecture out/frame-900.png --frame=900
```

---

## 📁 Architecture Compositions

| Composition | Resolution | FPS | Frames | Duration | File |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `OxmqArchitecture` | 1920x1080 | 30 | 960 | 32.0s | [`src/OxmqArchitecture.tsx`](./src/OxmqArchitecture.tsx) |

### Adding New Concepts / Videos
To create additional animated videos (e.g. *DAG Workflow Resolution*, *AIMD Adaptive Rate Limiter*, or *Zero-Data-Loss 200MB Backpressure*):
1. Create a new React component under `src/`.
2. Register it as a `<Composition>` in `src/Root.tsx`.
3. Preview with `npm start` and render with `npx remotion render src/index.ts <CompositionId> out/<name>.mp4`.
