import * as Y from 'yjs';
import * as decoding from 'lib0/decoding';
import * as awarenessProtocol from 'y-protocols/awareness';
import { describe, expect, it } from 'vitest';
import { AwarenessClocks, decodeAwarenessMessage, encodeAwarenessMessage, encodeAwarenessUpdate } from '../src/protocol';

function client(state: Record<string, unknown>) {
	const awareness = new awarenessProtocol.Awareness(new Y.Doc());
	awareness.setLocalState(state);
	return awareness;
}

describe('awareness messages', () => {
	it('decodes what y-protocols encodes and re-encodes it identically', () => {
		const a = client({ user: 'A' });
		const message = encodeAwarenessUpdate(a, [a.clientID]);
		const entries = decodeAwarenessMessage(message);
		expect(entries).toEqual([{ clientID: a.clientID, clock: a.meta.get(a.clientID)!.clock, state: JSON.stringify({ user: 'A' }) }]);
		expect(encodeAwarenessMessage(entries)).toEqual(message);
	});

	it('re-encoded subsets apply cleanly on a y-protocols peer', () => {
		const a = client({ user: 'A' });
		const b = client({ user: 'B' });
		const peer = new awarenessProtocol.Awareness(new Y.Doc());
		awarenessProtocol.applyAwarenessUpdate(peer, awarenessProtocol.encodeAwarenessUpdate(b, [b.clientID]), null);
		const [entry] = decodeAwarenessMessage(encodeAwarenessUpdate(a, [a.clientID]));
		const subset = encodeAwarenessMessage([entry]);
		awarenessProtocol.applyAwarenessUpdate(peer, decodeUpdatePayload(subset), null);
		expect(peer.getStates().get(a.clientID)).toEqual({ user: 'A' });
	});
});

describe('AwarenessClocks', () => {
	it('drops echoes of entries already seen on the stream', () => {
		const clocks = new AwarenessClocks();
		const update = { clientID: 7, clock: 3, state: '{"cursor":1}' };
		expect(clocks.fresh([update])).toEqual([update]); // never seen: the client's own update
		clocks.observe([update]); // read back from the stream and relayed
		expect(clocks.fresh([update])).toEqual([]); // the same entry coming back is an echo
		expect(clocks.fresh([{ ...update, clock: 2 }])).toEqual([]); // older is stale
		expect(clocks.fresh([{ ...update, clock: 4 }])).toHaveLength(1); // newer is a real change
	});

	it('keeps only the new entries of a mixed message', () => {
		const clocks = new AwarenessClocks();
		clocks.observe([{ clientID: 1, clock: 5, state: '{}' }]);
		const own = { clientID: 2, clock: 1, state: '{"user":"me"}' };
		expect(clocks.fresh([{ clientID: 1, clock: 5, state: '{}' }, own])).toEqual([own]);
	});

	it('treats a removal at an already seen clock as an echo', () => {
		const clocks = new AwarenessClocks();
		clocks.observe([{ clientID: 9, clock: 4, state: 'null' }]);
		expect(clocks.fresh([{ clientID: 9, clock: 4, state: 'null' }])).toEqual([]);
	});

	it('never moves a clock backwards', () => {
		const clocks = new AwarenessClocks();
		clocks.observe([{ clientID: 1, clock: 10, state: '{}' }]);
		clocks.observe([{ clientID: 1, clock: 3, state: '{}' }]);
		expect(clocks.fresh([{ clientID: 1, clock: 10, state: '{}' }])).toEqual([]);
	});
});

// The update payload inside an awareness message, as y-protocols' applyAwarenessUpdate expects it.
function decodeUpdatePayload(message: Uint8Array): Uint8Array {
	const decoder = decoding.createDecoder(message);
	decoding.readVarUint(decoder);
	return decoding.readVarUint8Array(decoder);
}
