import { currentLocationPollStatus, startCurrentLocationPoll } from '@/lib/current-location-poll';
import { locationAlertStatus, startLocationServiceChecker } from '@/lib/location-service-alerts';
import { startVworkApiPoll, vworkApiPollStatus } from '@/lib/vwork-api-poll';
import { startVworkGateUpdates, vworkGateUpdateStatus } from '@/lib/vwork-gate-queue';

export type DriverServiceRow = {
  name: string;
  running: boolean;
};

export type DriverServiceStart = {
  services: DriverServiceRow[];
  started: string[];
  errors: string[];
};

const STEPS: Array<[string, () => boolean, () => Promise<unknown>]> = [
  ['Track Solid locations', () => currentLocationPollStatus().running, () => startCurrentLocationPoll()],
  ['VWork jobs', () => vworkApiPollStatus().running, () => startVworkApiPoll()],
  ['Gate updates', () => vworkGateUpdateStatus().running, () => startVworkGateUpdates()],
  ['Location alerts', () => locationAlertStatus().checkerRunning, () => startLocationServiceChecker()],
];

let inflight: Promise<DriverServiceStart> | null = null;

/** Whether each driver service is running. Does not start anything. */
export function driverServiceStatus(): DriverServiceRow[] {
  return STEPS.map(([name, isRunning]) => ({ name, running: isRunning() }));
}

async function startStopped(): Promise<DriverServiceStart> {
  const started: string[] = [];
  const errors: string[] = [];
  for (const [name, isRunning, start] of STEPS) {
    if (isRunning()) continue;
    try {
      await start();
      started.push(name);
    } catch (err) {
      errors.push(`${name}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  if (errors.length > 0) console.error('[services] driver auto-start failed', errors.join('; '));
  else if (started.length > 0) console.info(`[services] driver app started ${started.join(', ')}`);
  return { services: driverServiceStatus(), started, errors };
}

/** Start any stopped polls. A driver login waits on this. A manual stop stays until the next login. */
export function ensureDriverServices(): Promise<DriverServiceStart> {
  if (process.env.NEXT_PHASE === 'phase-production-build') {
    return Promise.resolve({ services: driverServiceStatus(), started: [], errors: [] });
  }
  if (!inflight) {
    inflight = startStopped().finally(() => {
      inflight = null;
    });
  }
  return inflight;
}
