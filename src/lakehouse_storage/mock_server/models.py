"""Bounded controls for reproducible demo runs."""
from datetime import datetime, timezone
from typing import Literal

TABLE_SOURCES = {"mes_lims": "S2", "sensor_telemetry": "S1", "market_prices": "GDT", "market_indices": "FAO", "weather": "S4", "food_recalls": "S5", "market_reports": "USDA"}

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    seed: int = Field(default=42, ge=0, le=2**32 - 1)
    start_time: datetime = datetime(2026, 1, 15, tzinfo=timezone.utc)
    samples_per_batch: int = Field(default=1000, ge=1, le=50000)
    batches: int = Field(default=3, ge=1, le=200)
    scenario: Literal['normal', 'invalid-value', 'invalid-time', 'missing-field', 'transient-500', 'delay'] = 'normal'
    sources: list[str] = Field(default_factory=lambda: ['mes_lims', 'sensor_telemetry'])
    fao_variant: Literal['long', 'wide', 'both'] = 'long'
    end_time: datetime | None = None
    fault_table: str = 'sensor_telemetry'
    failure_attempts: int = Field(default=2, ge=0, le=5)
    delay_seconds: float = Field(default=0.2, ge=0, le=5)

    @model_validator(mode='after')
    def validate_run(self):
        if self.samples_per_batch * self.batches > 1_000_000:
            raise ValueError('run exceeds one million sensor samples')
        if self.start_time.tzinfo is None:
            self.start_time = self.start_time.replace(tzinfo=timezone.utc)
        if not self.sources or len(self.sources) != len(set(self.sources)) or set(self.sources) - TABLE_SOURCES.keys():
            raise ValueError('sources must be unique supported table names')
        if self.fault_table not in TABLE_SOURCES:
            raise ValueError('unknown fault table')
        from datetime import timedelta
        self.start_time = self.start_time.astimezone(timezone.utc)
        if self.end_time is None:
            self.end_time = self.start_time + timedelta(seconds=max(3600, self.samples_per_batch * self.batches))
        if self.end_time.tzinfo is None or self.end_time <= self.start_time:
            raise ValueError('end_time must follow start_time and include timezone')
        self.end_time = self.end_time.astimezone(timezone.utc)
        if (self.end_time - self.start_time).total_seconds() < self.samples_per_batch * self.batches and 'sensor_telemetry' in self.sources:
            raise ValueError('window too short for sensor samples')
        return self
