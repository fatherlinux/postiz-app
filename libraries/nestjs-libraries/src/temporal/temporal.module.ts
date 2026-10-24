import { TemporalModule } from 'nestjs-temporal-core';
import { socialIntegrationList } from '@gitroom/nestjs-libraries/integrations/integration.manager';
import { emailQueues } from '@gitroom/nestjs-libraries/temporal/email.queues';

// Every provider entry below becomes a Temporal worker, each with its own Rust
// core (@temporalio/core-bridge) and thread pair. Those allocations live outside
// the V8 heap, so --max-old-space-size does nothing to bound them. That is the
// right trade for a multi-tenant deployment; a single-tenant install pays for
// ~30 workers to service the handful of providers actually connected.
//
// POSTIZ_ACTIVE_PROVIDERS trims the provider workers to a comma-separated
// allowlist of identifiers, e.g. "x,linkedin,mastodon". Unset preserves
// upstream behaviour exactly. It composes with EXCLUDE_QUEUE: a queue must be
// allowed here and not excluded there. 'main' and the email queues are never
// subject to the allowlist.
//
// IMPORTANT: a provider with no worker has nobody polling its task queue, so
// its posts enqueue and never execute, silently. Connect a new provider in the
// UI, add it here, and restart. The boot log prints the active set so this is
// greppable when a post never lands.
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
  // Queues this worker server should NOT run, comma-separated
  // (e.g. EXCLUDE_QUEUE="reddit,x,twitch"). Use it to pin a queue to a single
  // server: exclude it on every server except the one that should own it.
  // Meant for the providers whose concurrency is too low to split (limit 1).
  const excludeQueues = (process.env.EXCLUDE_QUEUE || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  // How many worker servers share each (non-excluded) queue. Per-server
  // concurrency is divided by this so the GLOBAL concurrency stays correct.
  // 1 server => 1 (full), 2 servers => 2 (half each), 3 servers => 3, etc.
  const divider = Math.max(
    1,
    Number(process.env.WORKER_CONCURRENCY_DIVIDER) || 1
  );

  const activeProviders = parseActiveProviders();
  const providerQueues = [
    { identifier: 'main', maxConcurrentJob: undefined },
    ...socialIntegrationList,
  ]
    .filter((f) => f.identifier.indexOf('-') === -1)
    .map((integration) => ({
      integration,
      taskQueue: integration.identifier.split('-')[0],
    }))
    .filter(({ taskQueue }) => !excludeQueues.includes(taskQueue))
    .filter(
      ({ taskQueue }) =>
        activeProviders.length === 0 ||
        taskQueue === 'main' ||
        activeProviders.includes(taskQueue)
    );

  if (isWorkers && activeProviders.length > 0) {
    console.log(
      `[temporal] POSTIZ_ACTIVE_PROVIDERS set; starting ${
        providerQueues.length
      } provider workers: ${providerQueues
        .map(({ taskQueue }) => taskQueue)
        .join(', ')}`
    );

    const unknown = activeProviders.filter(
      (identifier) =>
        !providerQueues.some(({ taskQueue }) => taskQueue === identifier)
    );
    if (unknown.length > 0) {
      console.warn(
        `[temporal] POSTIZ_ACTIVE_PROVIDERS lists unknown or excluded provider(s): ${unknown.join(
          ', '
        )}; no worker started for them`
      );
    }
  }

  return TemporalModule.register({
    isGlobal: true,
    connection: {
      address: process.env.TEMPORAL_ADDRESS || 'localhost:7233',
      ...(process.env.TEMPORAL_TLS === 'true' ? { tls: true } : {}),
      ...(process.env.TEMPORAL_API_KEY
        ? { apiKey: process.env.TEMPORAL_API_KEY }
        : {}),
      namespace: process.env.TEMPORAL_NAMESPACE || 'default',
    },
    taskQueue: 'main',
    logLevel: 'error',
    ...(isWorkers
      ? {
          workers: [
            ...providerQueues.map(({ integration, taskQueue }) => {
              // Split the per-provider cap across the servers sharing this
              // queue. Floor (never below 1) so the global total never exceeds
              // the provider's limit. Providers whose limit is smaller than the
              // server count must be pinned via EXCLUDE_QUEUE instead.
              const concurrency = integration.maxConcurrentJob
                ? Math.max(
                    1,
                    Math.floor(integration.maxConcurrentJob / divider)
                  )
                : undefined;

              // Workflows only ever run on the `main` queue; the other Workers
              // are activity-only, so skip the workflow bundle (webpack build,
              // workflow thread + V8 isolate, sticky cache) on them.
              return {
                taskQueue,
                ...(taskQueue === 'main' ? { workflowsPath: path! } : {}),
                activityClasses: activityClasses!,
                autoStart: true,
                workerOptions: {
                  maxConcurrentActivityTaskExecutions: concurrency || 1000000,
                  // By default the SDK throttles heartbeat sends to 60s, so
                  // against the workflow's heartbeatTimeout one dropped send
                  // or a minute of event-loop lag eats most of the margin.
                  // Sending every 15s keeps the recorded heartbeat fresh even
                  // when individual sends fail or fire late.
                  maxHeartbeatThrottleInterval: '15s',
                  // Single-tenant workload: a handful of posts a day. The SDK
                  // default derives from heap_size_limit and lands around 135,
                  // sized for throughput we will never see. Only `main` runs
                  // workflows, so only it has a sticky cache to bound.
                  ...(taskQueue === 'main' ? { maxCachedWorkflows: 10 } : {}),
                },
              };
            }),
            // Activity-only Workers for emails, rate limited server side
            // across all workers to stay under the Resend limit
            ...Object.values(emailQueues)
              .filter(({ taskQueue }) => !excludeQueues.includes(taskQueue))
              .map(({ taskQueue, perSecond }) => ({
                taskQueue,
                activityClasses: activityClasses!,
                autoStart: true,
                workerOptions: {
                  maxConcurrentActivityTaskExecutions: 10,
                  maxTaskQueueActivitiesPerSecond: perSecond,
                },
              })),
          ],
        }
      : {}),
  });
};
