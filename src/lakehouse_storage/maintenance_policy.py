"""Conservative table policy for the stopped-ingest demo phase."""
from dataclasses import dataclass

from lakehouse_storage.service_config import require_namespace


@dataclass(frozen=True)
class MaintenancePolicy:
    table_uri: str
    allow_root: str
    file_count_threshold: int = 50
    small_file_ratio: float = 0.5
    target_file_size_mb: int = 128
    cooldown_seconds: float = 3600
    retention_hours: int = 168
    zorder_columns: tuple[str, ...] = ()
    window_start_hour: int = 0
    window_end_hour: int = 24
    cron_utc: str = '0 * * * *'

    def __post_init__(self):
        require_namespace(self.table_uri, self.allow_root)
        if self.file_count_threshold < 1 or self.target_file_size_mb < 1:
            raise ValueError('file threshold and target size must be positive')
        if not 0 <= self.small_file_ratio <= 1 or self.cooldown_seconds < 0:
            raise ValueError('invalid ratio/cooldown')
        if self.retention_hours < 168:
            raise ValueError('retention floor is 168 hours')
        if not 0 <= self.window_start_hour < self.window_end_hour <= 24:
            raise ValueError('invalid UTC maintenance window')
