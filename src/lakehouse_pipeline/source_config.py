"""Immutable source selection and half-open UTC collection windows."""
from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from lakehouse_storage.mock_server.models import TABLE_SOURCES


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    mode: Literal['mock', 'live', 'local-generator'] = 'mock'
    sources: tuple[str, ...] = tuple(TABLE_SOURCES)
    optional_sources: tuple[str, ...] = ()
    start: datetime
    end: datetime
    expected_recalls: int = Field(default=100, ge=1, le=10000)
    # Coordinates must be supplied explicitly for live weather.
    farms: tuple[tuple[str, float, float], ...] = ()

    @model_validator(mode='after')
    def validate_config(self):
        if self.start.tzinfo is None or self.end.tzinfo is None or self.start >= self.end:
            raise ValueError('explicit timezone-aware start < end required')
        if not self.sources or len(set(self.sources)) != len(self.sources) or set(self.sources) - TABLE_SOURCES.keys():
            raise ValueError('unknown, empty or duplicate sources')
        if set(self.optional_sources) - set(self.sources):
            raise ValueError('optional source must be selected')
        if self.mode == 'local-generator' and set(self.sources) - {'mes_lims', 'sensor_telemetry'}:
            raise ValueError('local-generator only simulates S1/S2')
        if self.mode == 'live' and set(self.sources) & {'mes_lims', 'sensor_telemetry'}:
            raise ValueError('no production SCADA/MES connector configured; use local-generator')
        return self

    def utc_window(self):
        return self.start.astimezone(timezone.utc), self.end.astimezone(timezone.utc)
