import { S2Logger } from './logger';

export async function uploadSnapshot(
	env: Env,
	room: string,
	ydocUpdate: Uint8Array,
	lastSeqNum: number | null | undefined,
	expectedETag: string | null,
	logger: S2Logger,
): Promise<void> {
	if (!env.R2_BUCKET) {
		logger.warn('No R2 bucket configured, skipping snapshot upload', { room }, 'SnapshotUpload');
		return;
	}

	const key = `snapshots/${encodeURIComponent(room)}/latest.bin`;
	logger.debug('Uploading snapshot to R2', { room, key, size: ydocUpdate.length, expectedETag }, 'SnapshotUpload');

	const putOptions: R2PutOptions = {
		customMetadata: {
			timestamp: Date.now().toString(),
			room: room,
			lastSeqNum: lastSeqNum?.toString() ?? '0',
		},
	};

	if (expectedETag === null) {
		putOptions.onlyIf = {
			etagDoesNotMatch: '*',
		};
	} else {
		putOptions.onlyIf = {
			etagMatches: expectedETag,
		};
	}

	try {
		await env.R2_BUCKET.put(key, ydocUpdate, putOptions);
		logger.info('Snapshot uploaded successfully', { room, key, lastSeqNum, expectedETag }, 'SnapshotUpload');
	} catch (error) {
		logger.warn(
			'Snapshot upload failed',
			{
				room,
				key,
				expectedETag,
				error: error instanceof Error ? error.message : String(error),
			},
			'SnapshotUpload',
		);

		throw error;
	}
}

export async function retrieveSnapshot(
	env: Env,
	room: string,
	logger: S2Logger,
): Promise<{ snapshot: Uint8Array; lastSeqNum: number; etag: string | null } | null> {
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
		etag: object.etag,
	};
}

export async function getSnapshotETag(env: Env, room: string, logger: S2Logger): Promise<string | null> {
	if (!env.R2_BUCKET) {
		logger.warn('No R2 bucket configured, cannot get ETag', { room }, 'SnapshotETag');
		return null;
	}

	const key = `snapshots/${encodeURIComponent(room)}/latest.bin`;
	logger.debug('Getting snapshot ETag from R2', { room, key }, 'SnapshotETag');

	const object = await env.R2_BUCKET.head(key);
	if (!object) {
		logger.debug('No snapshot found in R2 for ETag check', { room, key }, 'SnapshotETag');
		return null;
	}

	return object.etag;
}
