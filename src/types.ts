import { SequencedRecord } from '@s2-dev/streamstore/models/components';

export interface UserState {
	awarenessId: number | null;
	awarenessLastClock: number;
	room: string;
}

export function createUserState(room: string): UserState {
	return {
		awarenessId: null,
		awarenessLastClock: 0,
		room,
	};
}

export interface SnapshotState {
	firstRecordAge: number | null;
	currentFencingToken: string;
	trimSeqNum: number | null;
	lastProcessedFenceSeqNum: number;
	lastProcessedTrimSeqNum: number;
    lockBlocked: boolean;
	recordBuffer: SequencedRecord[];
}

export function createSnapshotState(): SnapshotState {
	return {
		firstRecordAge: null,
		currentFencingToken: '',
		trimSeqNum: null,
		lastProcessedFenceSeqNum: -1,
		lastProcessedTrimSeqNum: -1,
		lockBlocked: false,
		recordBuffer: [],
	};
}
