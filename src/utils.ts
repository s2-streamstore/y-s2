import { S2, AppendInput, AppendRecord, type ReadRecord } from '@s2-dev/streamstore';
import { fromUint8Array } from 'js-base64';

interface Config {
	maxBacklog: number;
	batchSize: number;
	lingerTime: number;
	leaseDuration: number;
	backlogBufferAge: number;
}

export function parseConfig(env: any): Config {
	return {
		maxBacklog: env.SNAPSHOT_BACKLOG_SIZE ? parseInt(env.SNAPSHOT_BACKLOG_SIZE, 10) : 400,
		batchSize: env.S2_BATCH_SIZE ? parseInt(env.S2_BATCH_SIZE, 10) : 8,
		lingerTime: env.S2_LINGER_TIME ? parseInt(env.S2_LINGER_TIME, 10) : 50,
		leaseDuration: env.LEASE_DURATION ? parseInt(env.LEASE_DURATION, 10) : 30,
		backlogBufferAge: env.BACKLOG_BUFFER_AGE ? parseInt(env.BACKLOG_BUFFER_AGE, 10) : 60_000,
	};
}

export function decodeBigEndian64AsNumber(bytes: Uint8Array): number {
	if (bytes.length !== 8) {
		throw new Error('Invalid input length, must be 8 bytes');
	}

	const value = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(0, false);
	return Number(value);
}

enum CommandType {
	FENCE = 'fence',
	TRIM = 'trim',
}

function compareHeaderValue(headerValue: string | Uint8Array, expected: string): boolean {
	if (typeof headerValue === 'string') {
		return headerValue === expected;
	}
	const decoded = new TextDecoder().decode(headerValue);
	return decoded === expected;
}

export function isCommandType(record: ReadRecord<'bytes'>, type: CommandType): boolean {
	if (!record.headers || record.headers.length !== 1) {
		return false;
	}
	const header = record.headers[0];
	if (!header) {
		return false;
	}
	return compareHeaderValue(header[0], '') && compareHeaderValue(header[1], type);
}

export function isFenceCommand(record: ReadRecord<'bytes'>): boolean {
	return isCommandType(record, CommandType.FENCE);
}

export function isTrimCommand(record: ReadRecord<'bytes'>): boolean {
	return isCommandType(record, CommandType.TRIM);
}

export function parseFencingToken(token: string): { id: string; deadline: number } {
	const [id, deadline] = token.split(' ');
	if (!id || !deadline) {
		throw new Error(`Invalid fencing token format: ${token}`);
	}
	return { id, deadline: Number(deadline) };
}

export function generateDeadlineFencingToken(leaseDuration: number): string {
	const newDeadline = Math.floor(Date.now() / 1000 + leaseDuration);
	const id = crypto.getRandomValues(new Uint8Array(12));
	const idBase64 = fromUint8Array(id);
	return `${idBase64} ${newDeadline}`;
}

export class Room {
	private s2Client: S2;
	private streamName: string;
	private s2Basin: string;

	constructor(s2Client: S2, streamName: string, s2Basin: string) {
		this.s2Client = s2Client;
		this.streamName = streamName;
		this.s2Basin = s2Basin;
	}

	async acquireLease(newFencingToken: string, prevFencingToken: string) {
		const stream = this.s2Client.basin(this.s2Basin).stream(this.streamName);
		return await stream.append(AppendInput.create([AppendRecord.fence(newFencingToken)], { fencingToken: prevFencingToken }));
	}

	async forceReleaseLease(currentFencingToken: string) {
		const stream = this.s2Client.basin(this.s2Basin).stream(this.streamName);
		return await stream.append(AppendInput.create([AppendRecord.fence('')], { fencingToken: currentFencingToken }));
	}

	async releaseLease(trimSeqNum: number, prevFencingToken: string) {
		const stream = this.s2Client.basin(this.s2Basin).stream(this.streamName);
		return await stream.append(
			AppendInput.create([AppendRecord.fence(''), AppendRecord.trim(trimSeqNum)], {
				fencingToken: prevFencingToken,
			}),
		);
	}
}
