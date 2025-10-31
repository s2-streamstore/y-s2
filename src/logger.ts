import { S2, AppendRecord } from '@s2-dev/streamstore';

export interface LogLevel {
	DEBUG: 'debug';
	INFO: 'info';
	WARN: 'warn';
	ERROR: 'error';
}

export const LogLevel: LogLevel = {
	DEBUG: 'debug',
	INFO: 'info',
	WARN: 'warn',
	ERROR: 'error',
} as const;

export type LogLevelType = LogLevel[keyof LogLevel];

export interface LogEntry {
	timestamp: number;
	workerId: string;
	level: LogLevelType;
	message: string;
	data?: any;
	source?: string;
	cfRay?: string;
	cfRequestId?: string;
}

enum LogMode {
	CONSOLE = 'CONSOLE',
	S2_SINGLE = 'S2_SINGLE',
	S2_SHARED = 'S2_SHARED',
}

export interface LoggerConfig {
	s2Client?: S2;
	s2Basin?: string;
	streamName?: string;
	workerId?: string;
	minLevel?: LogLevelType;
	request?: Request;
	logMode?: LogMode;
}

export class S2Logger {
	private s2Client?: S2;
	private s2Basin?: string;
	private streamName?: string;
	private workerId: string;
	private minLevel: LogLevelType;
	private cfRay?: string;
	private cfRequestId?: string;
	private logMode: LogMode;
	private readonly levelPriority = {
		debug: 0,
		info: 1,
		warn: 2,
		error: 3,
	};

	constructor(config: LoggerConfig) {
		this.logMode = config.logMode ?? LogMode.S2_SHARED;
		this.workerId = config.workerId ?? this.generateWorkerId();
		this.minLevel = config.minLevel ?? LogLevel.DEBUG;

		if (config.request) {
			this.cfRay = config.request.headers.get('cf-ray') ?? undefined;
			this.cfRequestId = config.request.headers.get('cf-request-id') ?? undefined;
		}

		if (this.logMode !== LogMode.CONSOLE) {
			this.s2Client = config.s2Client;
			this.s2Basin = config.s2Basin;

			if (this.logMode === LogMode.S2_SINGLE) {
				this.streamName = config.streamName ?? (this.cfRay ? `logs/worker-${this.cfRay}` : `logs/worker-${this.workerId}`);
			} else {
				this.streamName = config.streamName ?? 'logs/workers-shared';
			}
		}
	}

	private generateWorkerId(): string {
		const timestamp = Date.now();
		const random = Math.random().toString(36).substring(2, 8);
		return `worker-${timestamp}-${random}`;
	}

	private shouldLog(level: LogLevelType): boolean {
		return this.levelPriority[level] >= this.levelPriority[this.minLevel];
	}

	private createLogEntry(level: LogLevelType, message: string, data?: any, source?: string): LogEntry {
		return {
			timestamp: Date.now(),
			workerId: this.workerId,
			level,
			message,
			data,
			source,
			cfRay: this.cfRay,
			cfRequestId: this.cfRequestId,
		};
	}

	private async logEntry(entry: LogEntry): Promise<void> {
		if (this.logMode === LogMode.CONSOLE) {
			const timestamp = new Date(entry.timestamp).toISOString();
			const prefix = `[${timestamp}] ${entry.level.toUpperCase()} [${entry.workerId}]`;
			const message = `${prefix} ${entry.message}`;
			const data = entry.data ? ` | Data: ${JSON.stringify(entry.data)}` : '';
			const source = entry.source ? ` | Source: ${entry.source}` : '';
			const cf = entry.cfRay ? ` | CF-Ray: ${entry.cfRay}` : '';

			console.log(message + data + source + cf);
			return;
		}

		if (!this.s2Client || !this.s2Basin || !this.streamName) {
			console.error('S2 client not configured for log mode', this.logMode);
			return;
		}

		try {
			const basin = this.s2Basin;
			const stream = this.s2Client.basin(basin).stream(this.streamName);
			await stream.append(AppendRecord.make(JSON.stringify(entry)));
		} catch (error) {
			console.error('Failed to log to S2:', error);
		}
	}

	debug(message: string, data?: any, source?: string): void {
		if (!this.shouldLog(LogLevel.DEBUG)) {
			return;
		}
		const entry = this.createLogEntry(LogLevel.DEBUG, message, data, source);
		this.logEntry(entry);
	}

	info(message: string, data?: any, source?: string): void {
		if (!this.shouldLog(LogLevel.INFO)) {
			return;
		}
		const entry = this.createLogEntry(LogLevel.INFO, message, data, source);
		this.logEntry(entry);
	}

	warn(message: string, data?: any, source?: string): void {
		if (!this.shouldLog(LogLevel.WARN)) {
			return;
		}
		const entry = this.createLogEntry(LogLevel.WARN, message, data, source);
		this.logEntry(entry);
	}

	error(message: string, data?: any, source?: string): void {
		if (!this.shouldLog(LogLevel.ERROR)) {
			return;
		}
		const entry = this.createLogEntry(LogLevel.ERROR, message, data, source);
		this.logEntry(entry);
	}

	getWorkerId(): string {
		return this.workerId;
	}

	setLevel(level: LogLevelType): void {
		this.minLevel = level;
	}
}

export function createLogger(env: { S2_ACCESS_TOKEN?: string; S2_BASIN?: string; LOG_MODE?: string }, request?: Request): S2Logger {
	const logMode = (env.LOG_MODE ?? 'CONSOLE') as LogMode;

	return new S2Logger({
		s2Client: logMode !== LogMode.CONSOLE && env.S2_ACCESS_TOKEN ? new S2({ accessToken: env.S2_ACCESS_TOKEN }) : undefined,
		s2Basin: env.S2_BASIN,
		logMode,
		request,
	});
}
