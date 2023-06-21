import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, cast

from posthog.client import sync_execute
from posthog.models import Team
from posthog.models.session_recording.metadata import (
    DecompressedRecordingData,
    RecordingMetadata,
    SessionRecordingEvent,
    SessionRecordingEventSummary,
    SnapshotDataTaggedWithWindowId,
)
from posthog.session_recordings.session_recording_helpers import (
    decompress_chunked_snapshot_data,
    parse_snapshot_timestamp,
)
from posthog.utils import flatten


class SessionRecordingEvents:
    _session_recording_id: str
    _recording_start_time: Optional[datetime]
    _team: Team

    def __init__(self, session_recording_id: str, team: Team, recording_start_time: Optional[datetime] = None) -> None:
        self._session_recording_id = session_recording_id
        self._team = team
        self._recording_start_time = recording_start_time

    _recording_snapshot_query = """
        SELECT {fields}
        FROM session_recording_events
        PREWHERE
            team_id = %(team_id)s
            AND session_id = %(session_id)s
            {date_clause}
        ORDER BY timestamp
        {limit_param}
    """

    def get_recording_snapshot_date_clause(self) -> Tuple[str, Dict]:
        if self._recording_start_time:
            # If we can, we want to limit the time range being queried.
            # Theoretically, we shouldn't have to look before the recording start time,
            # but until we straighten out the recording start time logic, we should have a buffer
            return (
                """
                    AND toTimeZone(toDateTime(timestamp, 'UTC'), %(timezone)s) >= toDateTime(%(start_time)s, %(timezone)s) - INTERVAL 1 DAY
                    AND toTimeZone(toDateTime(timestamp, 'UTC'), %(timezone)s) <= toDateTime(%(start_time)s, %(timezone)s) + INTERVAL 2 DAY
            """,
                {"start_time": self._recording_start_time, "timezone": self._team.timezone},
            )
        return ("", {})

    def _query_recording_snapshots(self, include_snapshots=False) -> List[SessionRecordingEvent]:
        fields = ["session_id", "window_id", "distinct_id", "timestamp", "events_summary"]
        if include_snapshots:
            fields.append("snapshot_data")

        date_clause, date_clause_params = self.get_recording_snapshot_date_clause()
        query = self._recording_snapshot_query.format(date_clause=date_clause, fields=", ".join(fields), limit_param="")

        response = sync_execute(
            query, {"team_id": self._team.id, "session_id": self._session_recording_id, **date_clause_params}
        )

        return [
            SessionRecordingEvent(
                session_id=columns[0],
                window_id=columns[1],
                distinct_id=columns[2],
                timestamp=columns[3],
                events_summary=[json.loads(x) for x in columns[4]] if columns[4] else [],
                snapshot_data=json.loads(columns[5]) if len(columns) > 5 else None,
            )
            for columns in response
        ]

    # Fast constant time query that checks if session exists.
    def query_session_exists(self) -> bool:
        date_clause, date_clause_params = self.get_recording_snapshot_date_clause()
        query = self._recording_snapshot_query.format(
            date_clause=date_clause, fields="session_id", limit_param="LIMIT 1"
        )
        response = sync_execute(
            query, {"team_id": self._team.id, "session_id": self._session_recording_id, **date_clause_params}
        )
        return bool(response)

    def get_snapshots(self, limit, offset) -> Optional[DecompressedRecordingData]:
        all_snapshots = [
            SnapshotDataTaggedWithWindowId(
                window_id=recording_snapshot["window_id"], snapshot_data=recording_snapshot["snapshot_data"]
            )
            for recording_snapshot in self._query_recording_snapshots(include_snapshots=True)
        ]
        decompressed = decompress_chunked_snapshot_data(all_snapshots, limit, offset)

        if decompressed["snapshot_data_by_window_id"] == {}:
            return None
        return decompressed

    def get_metadata(self) -> Optional[RecordingMetadata]:
        snapshots = self._query_recording_snapshots(include_snapshots=False)
        events_summary = self._get_events_summary(snapshots)

        if len(events_summary) == 0:
            return None

        start_time = parse_snapshot_timestamp(events_summary[0]["timestamp"])
        end_time = parse_snapshot_timestamp(events_summary[-1]["timestamp"])
        click_count = len([x for x in events_summary if x["type"] == 3 and x["data"]["source"] == 2])
        keypress_count = len([x for x in events_summary if x["type"] == 3 and x["data"]["source"] == 5])
        urls: List[str] = [
            cast(str, x["data"]["href"]) for x in events_summary if isinstance(x.get("data", {}).get("href"), str)
        ]

        return RecordingMetadata(
            distinct_id=cast(str, snapshots[0]["distinct_id"]),
            start_time=start_time,
            end_time=end_time,
            duration=(end_time - start_time).seconds,
            click_count=click_count,
            keypress_count=keypress_count,
            urls=urls,
        )

    def _get_events_summary(self, snapshots: List[SessionRecordingEvent]) -> List[SessionRecordingEventSummary]:
        return sorted(
            list(flatten([cast(SessionRecordingEventSummary, x.get("events_summary", [])) for x in snapshots])),
            key=lambda x: x["timestamp"],
        )
