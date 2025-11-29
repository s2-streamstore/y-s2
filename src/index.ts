import { BatchTransform, S2, RangeNotSatisfiableError, AppendRecord, type ReadRecord } from '@s2-dev/streamstore';
import * as Y from 'yjs';
import * as decoding from 'lib0/decoding';
import * as awarenessProtocol from 'y-protocols/awareness';
import * as array from 'lib0/array';
import { createLogger, S2Logger } from './logger.js';
import {
	encodeAwarenessUpdate,
	encodeAwarenessUserDisconnected,
	encodeSyncStep1,
	encodeSyncStep2,
	messageAwareness,
	messageSync,
	messageSyncStep1,
	messageSyncStep2,
	messageSyncUpdate,
} from './protocol.js';
import { retrieveSnapshot, uploadSnapshot, getSnapshotETag } from './snapshot.js';
import {
	decodeBigEndian64AsNumber,
	generateDeadlineFencingToken,
	isFenceCommand,
	isTrimCommand,
	parseConfig,
	parseFencingToken,
	Room,
} from './utils.js';
import { createSnapshotState, createUserState, SnapshotState } from './types.js';

export interface Env {
	// S2 access token
	S2_ACCESS_TOKEN: string;
	// S2 basin name
	S2_BASIN: string;
	// R2 bucket for snapshots
	R2_BUCKET: R2Bucket;
	// Logging mode: CONSOLE | S2_SINGLE | S2_SHARED
	// CONSOLE: logs to console only
	// S2_SINGLE: logs to a single S2 stream with a unique worker ID
	// S2_SHARED: logs to a shared S2 stream with a worker ID
	LOG_MODE?: string;
	// Size of the record backlog to trigger a snapshot
	SNAPSHOT_BACKLOG_SIZE?: string;
	// Maximum age of a collected snapshot buffer before it is persisted to R2
	BACKLOG_BUFFER_AGE?: number;
	// Maximum batch size to reach before flushing to S2
	S2_BATCH_SIZE?: string;
	// Maximum time to wait before flushing a batch to S2
	S2_LINGER_TIME?: string;
	// Fencing token lease duration in seconds
	// Represented as: `{id} {leaseDeadline}`
	LEASE_DURATION?: number;
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		if (request.method === 'OPTIONS') {
			return new Response(null, {
				headers: {
					'Access-Control-Allow-Origin': '*',
					'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
					'Access-Control-Allow-Headers': 'Content-Type, Authorization',
				},
			});
		}

		if (url.pathname === '/auth/token') {
			return handleAuthToken(env, request);
		}

		if (url.pathname.startsWith('/ws/')) {
			return handleWebSocket(request, env);
		}

		if (url.pathname.match(/^\/auth\/perm\/(.+)\/(.+)$/)) {
			return handlePermissionCheck(request);
		}

		return new Response('Y-S2', { status: 200 });
	},
};

async function handleWebSocket(request: Request, env: Env): Promise<Response> {
	const logger = createLogger(env, request);

	if (request.headers.get('Upgrade') !== 'websocket') {
		return new Response('Expected Upgrade: websocket', { status: 426 });
	}

	const url = new URL(request.url);
	const roomName = url.searchParams.get('room');
	const authToken = url.searchParams.get('yauth');

	if (!roomName || !authToken) {
		logger.error('Missing required parameters', { room: !!roomName, authToken: !!authToken }, 'WebSocketValidation');
		return new Response('Missing room or auth token', { status: 400 });
	}

	try {
		if (!authToken || authToken.length === 0) {
			logger.error('Invalid auth token', { tokenLength: authToken?.length || 0 }, 'WebSocketAuth');
			return new Response('Invalid auth token', { status: 401 });
		}

		const pair = new WebSocketPair();
		const client = pair[0];
		const server = pair[1];
		server.accept();
		logger.info('WebSocket connection established', { roomName }, 'WebSocketConnection');

		const s2Client = new S2({ accessToken: env.S2_ACCESS_TOKEN });
		const streamName = `rooms/${encodeURIComponent(roomName)}/index`;

		const { maxBacklog, batchSize, lingerTime, leaseDuration, backlogBufferAge } = parseConfig(env);

		const room = new Room(s2Client, streamName, env.S2_BASIN);

		const ydoc = new Y.Doc();
		const awareness = new awarenessProtocol.Awareness(ydoc);
		awareness.setLocalState(null);

		const userState = createUserState(roomName);

		const initializeFromSnapshot = async () => {
			const checkpoint = await retrieveSnapshot(env, roomName, logger);

			ydoc.destroy();
			const newYdoc = new Y.Doc();
			Object.setPrototypeOf(ydoc, Object.getPrototypeOf(newYdoc));
			Object.assign(ydoc, newYdoc);

			awareness.destroy();
			const newAwareness = new awarenessProtocol.Awareness(ydoc);
			Object.setPrototypeOf(awareness, Object.getPrototypeOf(newAwareness));
			Object.assign(awareness, newAwareness);
			awareness.setLocalState(null);

			if (checkpoint?.snapshot) {
				logger.info(
					'Snapshot retrieved, applying to document',
					{
						room,
						lastSeqNum: checkpoint.lastSeqNum,
						snapshotSize: checkpoint.snapshot.length,
					},
					'SnapshotRestore',
				);
				Y.applyUpdateV2(ydoc, checkpoint.snapshot);
			} else {
				logger.debug('No snapshot found, starting with empty document', { room }, 'SnapshotRestore');
			}

			const newSnapshotState = createSnapshotState();

			const lastSeqNum = checkpoint?.lastSeqNum ?? 0;
			newSnapshotState.lastProcessedTrimSeqNum = lastSeqNum;
			const catchupSeqNum = checkpoint ? lastSeqNum + 1 : 0;

			return { catchupSeqNum, snapshotState: newSnapshotState };
		};

		let { catchupSeqNum, snapshotState } = await initializeFromSnapshot();

		const stream = s2Client.basin(env.S2_BASIN).stream(streamName);
		const tailResponse = await stream.checkTail();

		const tailSeqNum = tailResponse.tail.seq_num;

		logger.info(
			'Starting catchup from S2',
			{
				room,
				fromSeqNum: catchupSeqNum,
				tailSeqNum,
			},
			'S2Catchup',
		);

		const appendSession = await stream.appendSession();
		const batchTransform = new BatchTransform({
			maxBatchRecords: batchSize,
			lingerDurationMillis: lingerTime,
		});
		const batchWriter = batchTransform.writable.getWriter();
		const appendPipeline = batchTransform.readable.pipeTo(appendSession.writable);
		appendPipeline
			.catch((error: unknown) => {
				logger.error(
					'Append pipeline failed',
					{
						room,
						error: error instanceof Error ? error.message : String(error),
					},
					'AppendPipelineError',
				);
			})
			.finally(() => {
				appendSession.close().catch((error: unknown) => {
					logger.error(
						'Failed to close append session',
						{
							room,
							error: error instanceof Error ? error.message : String(error),
						},
						'AppendSessionCloseError',
					);
				});
			});

		const sendSyncMessages = (): void => {
			server.send(encodeSyncStep1(Y.encodeStateVector(ydoc)));
			server.send(encodeSyncStep2(Y.encodeStateAsUpdate(ydoc)));

			if (awareness.states.size > 0) {
				server.send(encodeAwarenessUpdate(awareness, array.from(awareness.states.keys())));
			}

			ydoc.destroy();
			awareness.destroy();
		};

		const readStreamWithRetry = async (): Promise<void> => {
			let currentCatchupSeqNum = catchupSeqNum;
			let currentSnapshotState = snapshotState;
			let attempts = 0;
			const maxAttempts = 3;

			while (attempts < maxAttempts) {
				attempts++;
				try {
					logger.info('Starting S2 event stream', { room, catchupSeqNum: currentCatchupSeqNum, attempt: attempts }, 'S2EventStream');
					const session = await stream.readSession({
						seq_num: currentCatchupSeqNum,
						as: 'bytes',
						clamp: true,
					});

					return await processEventStream(session, currentCatchupSeqNum, currentSnapshotState);
				} catch (err) {
					if (err instanceof RangeNotSatisfiableError) {
						logger.warn(
							'RangeNotSatisfiableError received - sequence number out of range',
							{
								room,
								requestedSeqNum: currentCatchupSeqNum,
								error: err instanceof Error ? err.message : String(err),
								attempt: attempts,
							},
							'RangeNotSatisfiableError',
						);

						// Re-fetch tail to get current sequence number
						const currentTail = await stream.checkTail();
						const actualTailSeqNum = currentTail.tail.seq_num;

						if (currentCatchupSeqNum < actualTailSeqNum) {
							logger.info(
								'Stale state from R2 - restarting from fresh snapshot',
								{
									room,
									requestedSeqNum: currentCatchupSeqNum,
									tailSeqNum: actualTailSeqNum,
									attempt: attempts,
								},
								'RestartFromSnapshot',
							);

							const newState = await initializeFromSnapshot();
							currentCatchupSeqNum = newState.catchupSeqNum;
							currentSnapshotState = newState.snapshotState;
							catchupSeqNum = currentCatchupSeqNum;
							snapshotState = currentSnapshotState;

							continue;
						} else {
							logger.warn('RangeNotSatisfiableError for sequence number higher than tail - not restarting', {
								room,
								requestedSeqNum: currentCatchupSeqNum,
								tailSeqNum: actualTailSeqNum,
								attempt: attempts,
							});
							throw err;
						}
					} else {
						logger.error(
							'S2 stream read error',
							{
								error: err instanceof Error ? err.message : String(err),
								room,
								attempt: attempts,
							},
							'S2StreamError',
						);
						throw err;
					}
				}
			}

			logger.error('Max retry attempts reached for stream reading', { room, maxAttempts }, 'MaxRetriesReached');
		};

		const processEventStream = async (
			session: AsyncIterable<ReadRecord<'bytes'>>,
			currentCatchupSeqNum: number,
			currentSnapshotState: SnapshotState,
		): Promise<void> => {
			let isCatchingUp = currentCatchupSeqNum < tailSeqNum;

			if (!isCatchingUp) {
				logger.info(
					'Already caught up, sending existing snapshot',
					{
						room,
						catchupSeqNum: currentCatchupSeqNum,
						tailSeqNum,
						recordCount: currentSnapshotState.recordBuffer.length,
					},
					'SnapshotSend',
				);

				sendSyncMessages();
			}

			for await (const r of session) {
				currentSnapshotState.trimSeqNum = r.seq_num;

				if (isFenceCommand(r)) {
					if (r.seq_num > currentSnapshotState.lastProcessedFenceSeqNum) {
						currentSnapshotState.currentFencingToken = r.body ? new TextDecoder().decode(r.body) : '';
						currentSnapshotState.lastProcessedFenceSeqNum = r.seq_num;
						if (!r.body) {
							currentSnapshotState.blocked = false;
						}
						logger.debug(
							'Received fencing token',
							{
								room,
								fencingToken: currentSnapshotState.currentFencingToken,
							},
							'FencingToken',
						);
					}
					continue;
				}

				if (!r.body) {
					continue;
				}

				if (isTrimCommand(r)) {
					const base64Body = btoa(String.fromCharCode(...r.body));
					const trimSeqNum = decodeBigEndian64AsNumber(base64Body);
					if (trimSeqNum > currentSnapshotState.lastProcessedTrimSeqNum) {
						currentSnapshotState.firstRecordAge = null;
						currentSnapshotState.lastProcessedTrimSeqNum = trimSeqNum;
						currentSnapshotState.recordBuffer = currentSnapshotState.recordBuffer.filter((rec) => rec.seq_num > trimSeqNum);
						logger.debug(
							'Received trim command',
							{
								room,
								seqNum: trimSeqNum,
							},
							'TrimCommand',
						);
					}
					continue;
				}

				currentSnapshotState.recordBuffer.push(r);

				if (currentSnapshotState.firstRecordAge === null) {
					currentSnapshotState.firstRecordAge = r.timestamp;
				}

				if (isCatchingUp) {
					if (r.seq_num + 1 >= tailSeqNum) {
						isCatchingUp = false;
						logger.info(
							'Catchup completed, processing records',
							{
								room,
								recordCount: currentSnapshotState.recordBuffer.length,
								finalSeqNum: r.seq_num,
							},
							'S2Catchup',
						);

						let docChanged = false;
						ydoc.once('afterTransaction', (tr) => {
							docChanged = tr.changed.size > 0;
						});

						ydoc.transact(() => {
							for (const record of currentSnapshotState.recordBuffer) {
								try {
									if (!record.body) {
										continue;
									}
									const recordBytes: Uint8Array = record.body;

									const decoder = decoding.createDecoder(recordBytes);
									const messageType = decoding.readUint8(decoder);

									if (messageType === messageSync) {
										const syncType = decoding.readUint8(decoder);
										if (syncType === messageSyncUpdate || syncType === messageSyncStep2) {
											const update = decoding.readVarUint8Array(decoder);
											Y.applyUpdate(ydoc, update);
										}
									} else if (messageType === messageAwareness) {
										const awarenessUpdate = decoding.readVarUint8Array(decoder);
										awarenessProtocol.applyAwarenessUpdate(awareness, awarenessUpdate, null);
									}
								} catch (err) {
									logger.error(
										'Failed to apply catchup record',
										{
											room,
											error: err instanceof Error ? err.message : String(err),
										},
										'CatchupError',
									);
								}
							}
						});

						logger.debug(
							'Catchup transaction completed',
							{
								room,
								docChanged,
								recordCount: currentSnapshotState.recordBuffer.length,
							},
							'CatchupComplete',
						);

						sendSyncMessages();
					}
					continue;
				}

				const recordBytes = r.body;
				server.send(recordBytes);

				const leaseExpired = (() => {
					if (!currentSnapshotState.currentFencingToken) {
						return true;
					}
					try {
						const { deadline } = parseFencingToken(currentSnapshotState.currentFencingToken);
						return Date.now() > deadline * 1000;
					} catch {
						logger.error('Invalid fencing token format', { room, token: currentSnapshotState.currentFencingToken }, 'FencingTokenError');
						return false;
					}
				})();

				const firstRecordExpired =
					currentSnapshotState.firstRecordAge !== null && Date.now() - currentSnapshotState.firstRecordAge > backlogBufferAge;

				const backlogSize =
					currentSnapshotState.trimSeqNum !== null ? currentSnapshotState.trimSeqNum + 1 - currentSnapshotState.lastProcessedTrimSeqNum : 0;

				const shouldSnapshot =
					(backlogSize >= maxBacklog && leaseExpired) ||
					(firstRecordExpired && leaseExpired) ||
					(currentSnapshotState.currentFencingToken && leaseExpired && backlogSize > 0);

				if (!shouldSnapshot || currentSnapshotState.blocked) {
					continue;
				}

				currentSnapshotState.blocked = true;

				takeSnapshot(
					env,
					leaseDuration,
					roomName,
					{ ...currentSnapshotState, recordBuffer: [...currentSnapshotState.recordBuffer] },
					room,
					logger,
				);
			}
		};

		(async () => {
			try {
				await readStreamWithRetry();
			} catch (err) {
				logger.error(
					'Failed to read stream after all retries',
					{ error: err instanceof Error ? err.message : String(err), room },
					'StreamReadFailure',
				);
			}
		})();

		server.addEventListener('message', async (event: MessageEvent) => {
			try {
				const buffer = event.data instanceof ArrayBuffer ? new Uint8Array(event.data) : new TextEncoder().encode(event.data);

				const messageType = buffer[0];

				if (messageType === messageSync) {
					const syncType = buffer[1];
					if (syncType === messageSyncStep1) {
						return;
					} else if (syncType === messageSyncStep2) {
						if (buffer.length >= 4) {
							buffer[1] = messageSyncUpdate;
						} else {
							return;
						}
					}
				}

				const shouldPropagate = (messageType === messageSync && buffer[1] === messageSyncUpdate) || messageType === messageAwareness;

				if (!shouldPropagate) {
					logger.warn('Unexpected message type', { messageType, syncType: buffer[1] }, 'UnexpectedMessage');
					return;
				}

				if (messageType === messageAwareness) {
					try {
						const decoder = decoding.createDecoder(buffer);
						decoding.readVarUint(decoder);
						decoding.readVarUint(decoder);
						const alen = decoding.readVarUint(decoder);
						const awId = decoding.readVarUint(decoder);

						if (alen === 1 && (userState.awarenessId === null || userState.awarenessId === awId)) {
							userState.awarenessId = awId;
							userState.awarenessLastClock = decoding.readVarUint(decoder);
						}
					} catch (err) {
						logger.error(
							'Failed to decode awareness message',
							{
								room,
								error: err instanceof Error ? err.message : String(err),
							},
							'AwarenessError',
						);
						return;
					}
				}
				batchWriter.write(AppendRecord.make(buffer)).catch((err: Error) => {
					logger.error(
						'Failed to submit message to batcher',
						{
							room,
							error: err.message,
						},
						'BatcherSubmitError',
					);
				});
			} catch (err) {
				logger.error(
					'Message processing error',
					{
						room,
						error: err instanceof Error ? err.message : String(err),
					},
					'MessageError',
				);
			}
		});

		server.addEventListener('close', async () => {
			logger.info(
				'WebSocket connection closed',
				{
					room,
					userId: userState.awarenessId,
				},
				'WebSocketClose',
			);

			if (userState.awarenessId !== null) {
				try {
					const disconnectMessage = encodeAwarenessUserDisconnected(userState.awarenessId, userState.awarenessLastClock);
					batchWriter.write(AppendRecord.make(disconnectMessage)).catch((err: Error) => {
						logger.error(
							'Failed to submit disconnect message',
							{
								room,
								userId: userState.awarenessId,
								error: err.message,
							},
							'DisconnectSubmitError',
						);
					});
					logger.info(
						'User disconnect message queued',
						{
							room,
							userId: userState.awarenessId,
							clock: userState.awarenessLastClock,
						},
						'UserDisconnect',
					);
				} catch (err) {
					logger.error(
						'Failed to create disconnect message',
						{
							room,
							userId: userState.awarenessId,
							error: err instanceof Error ? err.message : String(err),
						},
						'DisconnectError',
					);
				}
			}
			batchWriter
				.close()
				.catch((err: Error) => {
					logger.error(
						'Failed to close batch writer',
						{
							room,
							error: err.message,
						},
						'BatchWriterCloseError',
					);
				});
		});

		return new Response(null, {
			status: 101,
			webSocket: client,
		});
	} catch (err) {
		logger.error(
			'WebSocket handler failed',
			{
				error: err instanceof Error ? err.message : String(err),
				room: roomName,
			},
			'WebSocketHandlerError',
		);
		return new Response('Authentication failed', { status: 401 });
	}
}

async function handleAuthToken(env: Env, request: Request): Promise<Response> {
	const logger = createLogger(env, request);
	try {
		const demoToken = 'demo-jwt-token-12345';
		return new Response(demoToken, {
			headers: {
				'Access-Control-Allow-Origin': '*',
				'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
				'Access-Control-Allow-Headers': 'Content-Type, Authorization',
			},
		});
	} catch (error) {
		logger.error(
			'Auth token generation failed',
			{
				error: error instanceof Error ? error.message : String(error),
			},
			'AuthTokenError',
		);
		return new Response('Auth error', { status: 500 });
	}
}

async function handlePermissionCheck(request: Request): Promise<Response> {
	const url = new URL(request.url);

	const match = url.pathname.match(/^\/auth\/perm\/(.+)\/(.+)$/);
	if (!match?.[1] || !match[2]) {
		return new Response('Invalid path', { status: 400 });
	}

	const [, room, userid] = match;
	const permissionResponse = {
		yroom: decodeURIComponent(room),
		yaccess: 'rw',
		yuserid: userid,
	};
	return new Response(JSON.stringify(permissionResponse), {
		headers: {
			'Content-Type': 'application/json',
			'Access-Control-Allow-Origin': '*',
			'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
			'Access-Control-Allow-Headers': 'Content-Type, Authorization',
		},
	});
}

async function takeSnapshot(
	env: Env,
	leaseDuration: number,
	roomName: string,
	snapshotStateCopy: SnapshotState,
	room: Room,
	logger: S2Logger,
): Promise<void> {
	const newFencingToken = generateDeadlineFencingToken(leaseDuration);
	const currentETag = await getSnapshotETag(env, roomName, logger);

	try {
		await room.acquireLease(newFencingToken, snapshotStateCopy.currentFencingToken);
	} catch (err) {
		logger.error('Lease acquisition failed, skipping snapshot', { roomName, error: err }, 'LeaseAcquisitionError');
		return;
	}

	try {
		const snapshot = await retrieveSnapshot(env, roomName, logger);
		const startSeqNum = (snapshot?.lastSeqNum ?? -1) + 1;

		const ydoc = new Y.Doc();
		if (snapshot?.snapshot) {
			Y.applyUpdateV2(ydoc, snapshot.snapshot);
		}

		const { recordBuffer } = snapshotStateCopy;
		if (recordBuffer.length > 0 && recordBuffer[0].seq_num < startSeqNum) {
			logger.warn('Record buffer is stale, aborting snapshot', {
				roomName,
				firstRecordSeqNum: recordBuffer[0].seq_num,
				startSeqNum,
			});
			ydoc.destroy();
			return;
		}

		ydoc.transact(() => {
			for (const record of recordBuffer) {
				if (!record.body) {
					continue;
				}

				try {
					const bytes: Uint8Array = record.body;
					const decoder = decoding.createDecoder(bytes);
					const messageType = decoding.readUint8(decoder);

					if (messageType === messageSync) {
						const syncType = decoding.readUint8(decoder);
						if (syncType === messageSyncUpdate || syncType === messageSyncStep2) {
							const update = decoding.readVarUint8Array(decoder);
							Y.applyUpdate(ydoc, update);
						}
					}
				} catch (err) {
					logger.error('Failed to apply record during snapshot', {
						roomName,
						recordSeqNum: record.seq_num,
						error: err,
					});
				}
			}
		});

		const newSnapshot = Y.encodeStateAsUpdateV2(ydoc);
		await uploadSnapshot(env, roomName, newSnapshot, snapshotStateCopy.trimSeqNum, currentETag, logger);
		ydoc.destroy();

		await room.releaseLease(snapshotStateCopy.trimSeqNum!, newFencingToken);

		logger.info('Snapshot completed successfully', {
			roomName,
			recordsProcessed: recordBuffer.length,
			finalSeqNum: snapshotStateCopy.trimSeqNum,
		});
	} catch (err) {
		logger.error('Snapshot failed after acquiring lease', { roomName, error: err });
	} finally {
		try {
			await room.forceReleaseLease(newFencingToken);
		} catch (err) {
			logger.error('Failed to release lease', { roomName, error: err });
		}
	}
}
