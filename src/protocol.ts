/*
 * Portions of this file are derived from y-redis
 * Licensed under the GNU Affero General Public License v3.0
 * See <https://www.gnu.org/licenses/agpl-3.0.html> for license details.
 *
 * Modifications to this file are
 * Licensed under the GNU Affero General Public License v3.0
 */

import { encoding } from 'lib0';
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
