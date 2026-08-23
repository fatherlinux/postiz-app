import { TemporalModule } from 'nestjs-temporal-core';
import { socialIntegrationList } from '@gitroom/nestjs-libraries/integrations/integration.manager';

// Every entry in the workers array below becomes a full Temporal worker, and
// each worker carries its own Rust core (@temporalio/core-bridge), its own
// thread pair, and its own sticky workflow cache. Registering one per
// supported provider is the right call for a multi-tenant deployment where any
// provider might be in use by some customer. On a single-tenant install it
// means ~31 workers to service the handful of providers actually connected,
// and the cost is not small: the native allocations sit outside the V8 heap,
// so --max-old-space-size does nothing to bound them.
//
// POSTIZ_ACTIVE_PROVIDERS trims the worker set to a comma-separated list of
// provider identifiers, e.g. "x,linkedin,mastodon". Leaving it unset preserves
// upstream behaviour exactly (every provider gets a worker), so this is inert
// for anyone who does not opt in.
//
// IMPORTANT: a provider with no worker has nobody polling its task queue, so
// its posts enqueue in Temporal and never execute — silently. If you connect a
// new provider in the UI, add it here and restart. The boot log below prints
// the active set specifically so this is greppable when a post mysteriously
// never lands.
const parseActiveProviders = (): string[] =>
  (process.env.POSTIZ_ACTIVE_PROVIDERS || '')
    .split(',')
    .map((identifier) => identifier.trim())
    .filter(Boolean);

export const getTemporalModule = (
  isWorkers: boolean,
  path?: string,
  activityClasses?: any[]
) => {
  const activeProviders = parseActiveProviders();

  const workerIntegrations = [
    { identifier: 'main', maxConcurrentJob: undefined },
    ...socialIntegrationList,
  ]
    .filter((f) => f.identifier.indexOf('-') === -1)
    // 'main' carries the non-provider workflows and must always be present.
    .filter(
      (f) =>
        activeProviders.length === 0 ||
        f.identifier === 'main' ||
        activeProviders.includes(f.identifier)
    );

  if (isWorkers && activeProviders.length > 0) {
    console.log(
      `[temporal] POSTIZ_ACTIVE_PROVIDERS set; starting ${
        workerIntegrations.length
      } workers: ${workerIntegrations.map((f) => f.identifier).join(', ')}`
    );

    const unknown = activeProviders.filter(
      (identifier) =>
        !workerIntegrations.some((f) => f.identifier === identifier)
    );
    if (unknown.length > 0) {
      console.warn(
        `[temporal] POSTIZ_ACTIVE_PROVIDERS lists unknown provider(s): ${unknown.join(
          ', '
        )} — no worker started for them`
      );
    }
  }

  return TemporalModule.register({
    isGlobal: true,
    connection: {
      address: process.env.TEMPORAL_ADDRESS || 'localhost:7233',
      ...process.env.TEMPORAL_TLS === 'true' ? {tls: true} : {},
      ...process.env.TEMPORAL_API_KEY ? {apiKey: process.env.TEMPORAL_API_KEY} : {},
      namespace: process.env.TEMPORAL_NAMESPACE || 'default',
    },
    taskQueue: 'main',
    logLevel: 'error',
    ...(isWorkers
      ? {
          workers: workerIntegrations.map((integration) => ({
            taskQueue: integration.identifier.split('-')[0],
            workflowsPath: path!,
            activityClasses: activityClasses!,
            autoStart: true,
            workerOptions: {
              // Single-tenant workload: a handful of posts a day. The SDK
              // default is derived from heap_size_limit and lands around 135
              // per worker, which is sized for throughput we will never see.
              maxCachedWorkflows: 10,
              ...(integration.maxConcurrentJob
                ? {
                    maxConcurrentActivityTaskExecutions:
                      integration.maxConcurrentJob,
                  }
                : {}),
            },
          })),
        }
      : {}),
  });
};
