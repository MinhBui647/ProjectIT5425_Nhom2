"""Bounded controls for reproducible demo runs."""
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    seed: int = Field(default=42, ge=0, le=2**32 - 1)
    start_time: datetime = datetime(2026, 1, 15, tzinfo=timezone.utc)
    samples_per_batch: int = Field(default=1000, ge=1, le=50000)
    batches: int = Field(default=3, ge=1, le=200)
    scenario: Literal['normal', 'invalid-value', 'invalid-time', 'missing-field', 'transient-500', 'delay'] = 'normal'
    failure_attempts: int = Field(default=2, ge=0, le=5)
    delay_seconds: float = Field(default=0.2, ge=0, le=5)

    @model_validator(mode='after')
    def validate_run(self):
        if self.samples_per_batch * self.batches > 1_000_000:
            raise ValueError('run exceeds one million sensor samples')
        if self.start_time.tzinfo is None:
            self.start_time = self.start_time.replace(tzinfo=timezone.utc)
        return self
