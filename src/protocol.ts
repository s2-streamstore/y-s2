import { decoding, encoding } from 'lib0';
import * as awarenessProtocol from 'y-protocols/awareness';
import * as Y from 'yjs';
import * as array from 'lib0/array';

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

export function mergeMessages(messages: Uint8Array[]): Uint8Array[] {
	if (messages.length < 2) {
		return messages;
	}

	const updates: Uint8Array[] = [];
	const aw = new awarenessProtocol.Awareness(new Y.Doc());

	messages.forEach((m) => {
		try {
			const decoder = decoding.createDecoder(m);
			const messageType = decoding.readUint8(decoder);
			switch (messageType) {
				case messageSync: {
					const syncType = decoding.readUint8(decoder);
					if (syncType === messageSyncUpdate) {
						updates.push(decoding.readVarUint8Array(decoder));
					}
					break;
				}
				case messageAwareness: {
					awarenessProtocol.applyAwarenessUpdate(aw, decoding.readVarUint8Array(decoder), null);
					break;
				}
			}
		} catch (e) {
			console.error('Error parsing message for merging:', e);
		}
	});

	const result: Uint8Array[] = [];

	updates.length > 0 &&
		result.push(
			encoding.encode((encoder) => {
				encoding.writeVarUint(encoder, messageSync);
				encoding.writeVarUint(encoder, messageSyncUpdate);
				encoding.writeVarUint8Array(encoder, Y.mergeUpdates(updates));
			}),
		);

	aw.states.size > 0 &&
		result.push(
			encoding.encode((encoder) => {
				encoding.writeVarUint(encoder, messageAwareness);
				encoding.writeVarUint8Array(encoder, awarenessProtocol.encodeAwarenessUpdate(aw, array.from(aw.getStates().keys())));
			}),
		);

	return result;
}
