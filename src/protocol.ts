/*
 * Portions of this file are derived from y-redis
 * Licensed under the GNU Affero General Public License v3.0
 * See <https://www.gnu.org/licenses/agpl-3.0.html> for license details.
 *
 * Modifications to this file are
 * Licensed under the GNU Affero General Public License v3.0
 */

import { decoding, encoding } from 'lib0';
import * as awarenessProtocol from 'y-protocols/awareness';

export const messageSync = 0;
export const messageSyncStep1 = 0;
export const messageSyncStep2 = 1;
export const messageSyncUpdate = 2;
export const messageAwareness = 1;

export const encodeSyncStep1 = (sv: Uint8Array): Uint8Array =>
	encoding.encode((encoder) => {
		encoding.writeVarUint(encoder, messageSync);
		encoding.writeVarUint(encoder, messageSyncStep1);
		encoding.writeVarUint8Array(encoder, sv);
	});

export const encodeSyncStep2 = (diff: Uint8Array): Uint8Array =>
	encoding.encode((encoder) => {
		encoding.writeVarUint(encoder, messageSync);
		encoding.writeVarUint(encoder, messageSyncStep2);
		encoding.writeVarUint8Array(encoder, diff);
	});

export const encodeAwarenessUpdate = (awareness: awarenessProtocol.Awareness, changedClients: number[]): Uint8Array =>
	encoding.encode((encoder) => {
		encoding.writeVarUint(encoder, messageAwareness);
		encoding.writeVarUint8Array(encoder, awarenessProtocol.encodeAwarenessUpdate(awareness, changedClients));
	});

export const encodeAwarenessUserDisconnected = (clientid: number, lastClock: number): Uint8Array =>
	encoding.encode((encoder) => {
		encoding.writeVarUint(encoder, messageAwareness);
		encoding.writeVarUint8Array(
			encoder,
			encoding.encode((encoder) => {
				encoding.writeVarUint(encoder, 1);
				encoding.writeVarUint(encoder, clientid);
				encoding.writeVarUint(encoder, lastClock + 1);
				encoding.writeVarString(encoder, JSON.stringify(null));
			}),
		);
	});

export interface AwarenessEntry {
	clientID: number;
	clock: number;
	// JSON-encoded awareness state ('null' when the client was removed).
	state: string;
}

// Decodes an awareness message (`messageAwareness` followed by an encoded awareness update).
export function decodeAwarenessMessage(message: Uint8Array): AwarenessEntry[] {
	const decoder = decoding.createDecoder(message);
	decoding.readVarUint(decoder);
	const update = decoding.createDecoder(decoding.readVarUint8Array(decoder));
	const count = decoding.readVarUint(update);
	const entries: AwarenessEntry[] = [];
	for (let i = 0; i < count; i++) {
		const clientID = decoding.readVarUint(update);
		const clock = decoding.readVarUint(update);
		const state = decoding.readVarString(update);
		entries.push({ clientID, clock, state });
	}
	return entries;
}

export const encodeAwarenessMessage = (entries: AwarenessEntry[]): Uint8Array =>
	encoding.encode((encoder) => {
		encoding.writeVarUint(encoder, messageAwareness);
		encoding.writeVarUint8Array(
			encoder,
			encoding.encode((update) => {
				encoding.writeVarUint(update, entries.length);
				for (const { clientID, clock, state } of entries) {
					encoding.writeVarUint(update, clientID);
					encoding.writeVarUint(update, clock);
					encoding.writeVarString(update, state);
				}
			}),
		);
	});

// y-protocols considers a client gone after 30s without its 15s heartbeat, and peers echo that
// removal right away, so an entry not refreshed for this long can no longer be echoed.
const AWARENESS_ENTRY_TTL_MS = 60_000;

/**
 * Latest awareness clock per client seen on the room stream.
 *
 * y-websocket clients re-broadcast every awareness update they receive, including the ones relayed
 * to them from the stream. Awareness clocks only move forward, so an entry whose clock is not newer
 * than what the stream already has is an echo and must not be appended again: otherwise every
 * update is written once more per connected client and fanned out to all of them.
 *
 * Clients that stop updating are forgotten after a while, so long-lived connections in rooms with
 * many comings and goings do not accumulate an entry per past visitor.
 */
export class AwarenessClocks {
	private readonly clocks = new Map<number, { clock: number; seenAt: number }>();
	private lastSweep = 0;

	observe(entries: AwarenessEntry[], now = Date.now()): void {
		for (const { clientID, clock } of entries) {
			if (clock > (this.clocks.get(clientID)?.clock ?? -1)) {
				this.clocks.set(clientID, { clock, seenAt: now });
			}
		}
		if (now - this.lastSweep >= AWARENESS_ENTRY_TTL_MS) {
			this.lastSweep = now;
			for (const [clientID, { seenAt }] of this.clocks) {
				if (now - seenAt > AWARENESS_ENTRY_TTL_MS) {
					this.clocks.delete(clientID);
				}
			}
		}
	}

	fresh(entries: AwarenessEntry[]): AwarenessEntry[] {
		return entries.filter(({ clientID, clock }) => clock > (this.clocks.get(clientID)?.clock ?? -1));
	}
}
