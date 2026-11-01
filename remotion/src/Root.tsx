import React from "react";
import { Composition } from "remotion";
import { OxmqArchitecture } from "./OxmqArchitecture";
import { JobLifecycleMotion } from "./compositions/JobLifecycleMotion";
import { DagWorkflowMotion } from "./compositions/DagWorkflowMotion";
import { BatchDequeueMotion } from "./compositions/BatchDequeueMotion";
import { BackpressureMotion } from "./compositions/BackpressureMotion";
import { LockWatchdogDiagram } from "./compositions/LockWatchdogDiagram";
import { VirtualGroupsDiagram } from "./compositions/VirtualGroupsDiagram";
import { DistributedTracingDiagram } from "./compositions/DistributedTracingDiagram";
import { TransactionalOutboxDiagram } from "./compositions/TransactionalOutboxDiagram";
import { DistributedCancellationDiagram } from "./compositions/DistributedCancellationDiagram";
import { DistributedRelayDiagram } from "./compositions/DistributedRelayDiagram";
import { DisasterRecoveryDiagram } from "./compositions/DisasterRecoveryDiagram";
import { NamespaceHierarchyDiagram } from "./compositions/NamespaceHierarchyDiagram";
import { MemoryOptimizationDiagram } from "./compositions/MemoryOptimizationDiagram";
import { RequestReplyDiagram } from "./compositions/RequestReplyDiagram";

export const Root: React.FC = () => {
  return (
    <>
      {/* Motion Compositions (Videos & Animated GIFs) */}
      <Composition
        id="OxmqArchitecture"
        component={OxmqArchitecture}
        durationInFrames={960}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="JobLifecycleMotion"
        component={JobLifecycleMotion}
        durationInFrames={360}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="DagWorkflowMotion"
        component={DagWorkflowMotion}
        durationInFrames={360}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="BatchDequeueMotion"
        component={BatchDequeueMotion}
        durationInFrames={300}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="BackpressureMotion"
        component={BackpressureMotion}
        durationInFrames={300}
        fps={30}
        width={1920}
        height={1080}
      />

      {/* High-Resolution Static Architecture Diagrams */}
      <Composition
        id="LockWatchdogDiagram"
        component={LockWatchdogDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="VirtualGroupsDiagram"
        component={VirtualGroupsDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="DistributedTracingDiagram"
        component={DistributedTracingDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="TransactionalOutboxDiagram"
        component={TransactionalOutboxDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="DistributedCancellationDiagram"
        component={DistributedCancellationDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="DistributedRelayDiagram"
        component={DistributedRelayDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="DisasterRecoveryDiagram"
        component={DisasterRecoveryDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="NamespaceHierarchyDiagram"
        component={NamespaceHierarchyDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="MemoryOptimizationDiagram"
        component={MemoryOptimizationDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
      <Composition
        id="RequestReplyDiagram"
        component={RequestReplyDiagram}
        durationInFrames={1}
        fps={30}
        width={1920}
        height={1080}
      />
    </>
  );
};
