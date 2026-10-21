import { AppendRecord, type ReadRecord } from '@s2-dev/streamstore';
import type { Env } from './index';

// S2 caps a record at 1 MiB, but a single Yjs message can be larger: a big paste, or the sync step 2
// a reconnecting client sends with everything the stream is missing. Such messages are stored in R2
// and the stream carries a pointer record instead: a `y-s2: blob` header with the object key as body.
const POINTER_HEADER = 'y-s2';
const POINTER_HEADER_VALUE = 'blob';

export async function toAppendRecord(env: Env, room: string, message: Uint8Array): Promise<AppendRecord> {
	if (message.length <= AppendRecord.maxBodyBytes()) {
		return AppendRecord.bytes({ body: message });
	}
	// Content-addressed, so a client re-sending the same message (e.g. after reconnecting) reuses the object.
	const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', message));
	const hash = Array.from(digest, (b) => b.toString(16).padStart(2, '0')).join('');
	const key = `blobs/${encodeURIComponent(room)}/${hash}`;
	await env.R2_BUCKET.put(key, message);
	return AppendRecord.bytes({ body: new TextEncoder().encode(key), headers: [[POINTER_HEADER, POINTER_HEADER_VALUE]] });
}

export function isBlobPointer(record: ReadRecord<'bytes'>): boolean {
	if (record.headers.length !== 1) {
		return false;
	}
	const [name, value] = record.headers[0];
	const decoder = new TextDecoder();
	return decoder.decode(name) === POINTER_HEADER && decoder.decode(value) === POINTER_HEADER_VALUE;
}

// Returns the record with the stored message as its body, or null if the object is missing.
export async function resolveBlobPointer(env: Env, record: ReadRecord<'bytes'>): Promise<ReadRecord<'bytes'> | null> {
	const object = await env.R2_BUCKET.get(new TextDecoder().decode(record.body));
	if (!object) {
		return null;
	}
	return { ...record, body: new Uint8Array(await object.arrayBuffer()) };
}
