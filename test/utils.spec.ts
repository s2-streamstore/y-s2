import { describe, expect, it } from 'vitest';
import { generateDeadlineFencingToken, parseFencingToken } from '../src/utils';

describe('parseFencingToken', () => {
	it('parses a generated token', () => {
		const { deadline } = parseFencingToken(generateDeadlineFencingToken(30));
		expect(Number.isFinite(deadline)).toBe(true);
	});

	it('rejects a token whose deadline is not a number', () => {
		expect(() => parseFencingToken('abc not-a-deadline')).toThrow('Invalid fencing token format');
	});
});
