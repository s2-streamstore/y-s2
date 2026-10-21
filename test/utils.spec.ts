import { AppendRecord, type ReadRecord } from '@s2-dev/streamstore';
import { describe, expect, it } from 'vitest';
import { LogMode, parseLogMode } from '../src/logger';
import { decodeBigEndian64AsNumber, generateDeadlineFencingToken, isFenceCommand, isTrimCommand, parseFencingToken } from '../src/utils';

// Simulates reading back an appended record with `as: 'bytes'`.
function asReadRecord(record: AppendRecord, seqNum = 0): ReadRecord<'bytes'> {
	const encoder = new TextEncoder();
	const toBytes = (v: string | Uint8Array) => (typeof v === 'string' ? encoder.encode(v) : v);
	return {
		seqNum,
		body: toBytes(record.body),
		headers: (record.headers ?? []).map(([k, v]) => [toBytes(k), toBytes(v)] as const),
		timestamp: new Date(),
	};
}

describe('command records', () => {
	it('recognizes fence records', () => {
		const token = generateDeadlineFencingToken(30);
		const record = asReadRecord(AppendRecord.fence(token));
		expect(isFenceCommand(record)).toBe(true);
		expect(isTrimCommand(record)).toBe(false);
		expect(new TextDecoder().decode(record.body)).toBe(token);
	});

	it('treats a released lease as an empty fence body', () => {
		const record = asReadRecord(AppendRecord.fence(''));
		expect(isFenceCommand(record)).toBe(true);
		expect(record.body.length).toBe(0);
	});

	it('round-trips trim sequence numbers', () => {
		for (const seqNum of [0, 1, 400, 2 ** 40 + 7, Number.MAX_SAFE_INTEGER]) {
			const record = asReadRecord(AppendRecord.trim(seqNum));
			expect(isTrimCommand(record)).toBe(true);
			expect(isFenceCommand(record)).toBe(false);
			expect(decodeBigEndian64AsNumber(record.body)).toBe(seqNum);
		}
	});

	it('decodes trim bodies that are a view into a larger buffer', () => {
		const backing = new Uint8Array(16);
		new DataView(backing.buffer).setBigUint64(4, 1234n, false);
		expect(decodeBigEndian64AsNumber(backing.subarray(4, 12))).toBe(1234);
	});

	it('does not treat regular records as commands', () => {
		const record = asReadRecord(AppendRecord.bytes({ body: new Uint8Array([0, 2, 1, 0]) }));
		expect(isFenceCommand(record)).toBe(false);
		expect(isTrimCommand(record)).toBe(false);
	});
});

describe('fencing tokens', () => {
	it('generates parseable tokens that fit S2 limits', () => {
		const token = generateDeadlineFencingToken(30);
		expect(new TextEncoder().encode(token).length).toBeLessThanOrEqual(36);
		const { deadline } = parseFencingToken(token);
		expect(deadline).toBeGreaterThan(Date.now() / 1000);
	});
});

describe('LOG_MODE parsing', () => {
	it('accepts any casing and surrounding whitespace', () => {
		expect(parseLogMode('console')).toBe(LogMode.CONSOLE);
		expect(parseLogMode(' s2_single ')).toBe(LogMode.S2_SINGLE);
		expect(parseLogMode('S2_Shared')).toBe(LogMode.S2_SHARED);
	});

	it('falls back to CONSOLE when unset or unknown', () => {
		expect(parseLogMode(undefined)).toBe(LogMode.CONSOLE);
		expect(parseLogMode('')).toBe(LogMode.CONSOLE);
		expect(parseLogMode('CONSLE')).toBe(LogMode.CONSOLE);
	});
});
