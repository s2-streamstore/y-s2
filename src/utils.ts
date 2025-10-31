import { S2, SequencedRecord, AppendRecord } from '@s2-dev/streamstore';
import { fromUint8Array } from 'js-base64';
import { S2Logger } from './logger';
import { mergeMessages } from './protocol';

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

export function encodeBigEndian64(num: number): string {
	const buffer = new ArrayBuffer(8);
	const view = new DataView(buffer);
	view.setBigUint64(0, BigInt(num), false);
	const bytes = new Uint8Array(buffer);
	return btoa(String.fromCharCode(...bytes));
}

export function decodeBigEndian64AsNumber(base64: string): number {
	const binary = atob(base64);

	if (binary.length !== 8) {
		throw new Error('Invalid input length, must be 8 bytes');
	}

	const buffer = new ArrayBuffer(8);
	const bytes = new Uint8Array(buffer);

	for (let i = 0; i < 8; i++) {
		bytes[i] = binary.charCodeAt(i);
	}
	const value = new DataView(buffer).getBigUint64(0, false);
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

export function isCommandType<Format extends 'string' | 'bytes' = 'string'>(
	record: SequencedRecord<Format>,
	type: CommandType,
): boolean {
	if (!record.headers || record.headers.length !== 1) {return false;}
	const header = record.headers[0];
	if (!header) {return false;}
	return compareHeaderValue(header[0] as any, '') && compareHeaderValue(header[1] as any, type);
}

export function isFenceCommand<Format extends 'string' | 'bytes' = 'string'>(
	record: SequencedRecord<Format>,
): boolean {
	return isCommandType(record, CommandType.FENCE);
}

export function isTrimCommand<Format extends 'string' | 'bytes' = 'string'>(
	record: SequencedRecord<Format>,
): boolean {
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
		return await stream.append(
			AppendRecord.make(newFencingToken, [['', 'fence']]),
			{ fencing_token: prevFencingToken },
		);
	}

	async forceReleaseLease(currentFencingToken: string) {
		const stream = this.s2Client.basin(this.s2Basin).stream(this.streamName);
		return await stream.append(
			AppendRecord.make('', [['', 'fence']]),
			{ fencing_token: currentFencingToken },
		);
	}

	async releaseLease(trimSeqNum: number, prevFencingToken: string) {
		const stream = this.s2Client.basin(this.s2Basin).stream(this.streamName);
		return await stream.append(
			[
				AppendRecord.make('', [['', 'fence']]),
				AppendRecord.make(encodeBigEndian64(trimSeqNum), [['', 'trim']]),
			],
			{ fencing_token: prevFencingToken },
		);
	}
}

export class MessageBatcher {
	private messageBatch: Uint8Array[] = [];
	private batchTimeout: NodeJS.Timeout | null = null;

	constructor(
		private readonly s2Client: S2,
		private readonly streamName: string,
		private readonly s2Basin: string,
		private readonly logger: S2Logger,
		private readonly room: string,
		private readonly batchSize: number,
		private readonly lingerTime: number,
	) {}

	addMessage(message: Uint8Array): void {
		this.messageBatch.push(message);

		if (this.messageBatch.length >= this.batchSize) {
			this.flush();
		} else {
			this.resetTimeout();
		}
	}

	async flush(): Promise<void> {
		if (this.messageBatch.length === 0) {return;}

		const batch = [...this.messageBatch];
		this.clearBatch();
		this.clearTimeout();

		try {
			const messagesToSend = mergeMessages(batch);
			const stream = this.s2Client.basin(this.s2Basin).stream(this.streamName);
			const records = messagesToSend.map((msg) => AppendRecord.make(msg));
			await stream.append(records);
		} catch (err) {
			this.logger.error(
				'Failed to append batch to S2',
				{
					room: this.room,
					streamName: this.streamName,
					error: err instanceof Error ? err.message : String(err),
				},
				'S2AppendError',
			);
		}
	}

	private clearBatch(): void {
		this.messageBatch = [];
	}

	private clearTimeout(): void {
		if (this.batchTimeout) {
			clearTimeout(this.batchTimeout);
			this.batchTimeout = null;
		}
	}

	private resetTimeout(): void {
		this.clearTimeout();
		this.batchTimeout = setTimeout(() => this.flush(), this.lingerTime);
	}
}
