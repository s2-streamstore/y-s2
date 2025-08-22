import { S2Logger } from './logger';

export async function uploadSnapshot(
	env: Env,
	room: string,
	ydocUpdate: Uint8Array,
	lastSeqNum: number | null | undefined,
	logger: S2Logger,
): Promise<void> {
	if (!env.R2_BUCKET) {
		logger.warn('No R2 bucket configured, skipping snapshot upload', { room }, 'SnapshotUpload');
		return;
	}

	const key = `snapshots/${encodeURIComponent(room)}/latest.bin`;
	logger.debug('Uploading snapshot to R2', { room, key, size: ydocUpdate.length }, 'SnapshotUpload');

	await env.R2_BUCKET.put(key, ydocUpdate, {
		customMetadata: {
			timestamp: Date.now().toString(),
			room: room,
			lastSeqNum: lastSeqNum?.toString() ?? '0',
		},
	});
	logger.info('Snapshot uploaded successfully', { room, key, lastSeqNum }, 'SnapshotUpload');
}

export async function retrieveSnapshot(
	env: Env,
	room: string,
	logger: S2Logger,
): Promise<{ snapshot: Uint8Array; lastSeqNum: number } | null> {
	if (!env.R2_BUCKET) {
		logger.warn('No R2 bucket configured, no snapshot to retrieve', { room }, 'SnapshotRetrieve');
		return null;
	}

	const key = `snapshots/${encodeURIComponent(room)}/latest.bin`;
	logger.debug('Retrieving snapshot from R2', { room, key }, 'SnapshotRetrieve');

	const object = await env.R2_BUCKET.get(key);
	if (!object) {
		logger.debug('No snapshot found in R2', { room, key }, 'SnapshotRetrieve');
		return null;
	}

	const arrayBuffer = await object.arrayBuffer();
	const lastSeqNum = parseInt(object.customMetadata?.lastSeqNum || '0', 10);
	return {
		snapshot: new Uint8Array(arrayBuffer),
		lastSeqNum,
	};
}
