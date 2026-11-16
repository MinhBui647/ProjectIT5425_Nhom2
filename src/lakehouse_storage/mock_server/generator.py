"""Reuse raw generators while freezing wall-clock metadata."""
from datetime import timedelta

import polars as pl

from lakehouse_storage.generate_mes_data import generate_mes_batch
from lakehouse_storage.mock_sensor_stream import generate_sensor_batch


def generate_batches(request, run_id):
    start = request.start_time
    batch_id = f'DEMO_{run_id}'
    mes = generate_mes_batch(start, batch_id=batch_id, production_start=start,
                             production_end=start + timedelta(seconds=request.samples_per_batch * request.batches),
                             seed=request.seed)
    yield 'mes_lims', mes.with_columns(pl.lit(start.isoformat()).alias('_generated_at')).to_dicts()
    for i in range(request.batches):
        frame = generate_sensor_batch(request.samples_per_batch, batch_id=batch_id,
                                      start_time=start + timedelta(seconds=i * request.samples_per_batch),
                                      seed=(request.seed + i) % 2**32)
        yield 'sensor_telemetry', frame.with_columns(pl.lit(start.isoformat()).alias('_generated_at')).to_dicts()
