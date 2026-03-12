import * as Sentry from "@sentry/nextjs";

// Extract headers for Sentry tracing for fetch requests
export function sentryFetchHeaders() {
  const traceData = Sentry.getTraceData();
  const sentryTraceHeader = traceData["sentry-trace"];
  const sentryBaggageHeader = traceData["baggage"];

  if (!sentryTraceHeader || !sentryBaggageHeader) {
    return {};
  }

  return {
    "sentry-trace": sentryTraceHeader,
    "baggage": sentryBaggageHeader,
  };
}
