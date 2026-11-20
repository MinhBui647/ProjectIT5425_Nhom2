"""Render measured layout/latency/throughput evidence; optional plotting imports."""
import json
from pathlib import Path


def chart_data(metrics):
    entries = metrics['configurations']
    if not entries or not all(e['correctness_verified'] for e in entries):
        raise ValueError('report requires verified query results')
    return {'batch_sizes': [e['batch_size'] for e in entries],
            'files_before': [e['before']['file_count'] for e in entries],
            'files_after': [e['after']['file_count'] for e in entries],
            'throughput': [e['input_rows_per_second'] for e in entries]}


def render_report(metrics_path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    metrics_path = Path(metrics_path)
    metrics = json.loads(metrics_path.read_text())
    mapped = chart_data(metrics)
    entries = metrics['configurations']
    output = metrics_path.parent / 'figures'
    output.mkdir(exist_ok=True)
    paths = []
    labels = [f'{size:,}' for size in mapped['batch_sizes']]
    positions = np.arange(len(labels))
    colors = ['#285e8e', '#d27b37']

    def save(figure, name):
        figure.tight_layout()
        for extension in ('png', 'svg'):
            path = output / f'{name}.{extension}'
            figure.savefig(path, dpi=160, bbox_inches='tight')
            paths.append(path)
        plt.close(figure)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(positions-0.18, mapped['files_before'], 0.36, label='Baseline', color=colors[0])
    ax.bar(positions+0.18, mapped['files_after'], 0.36, label='Compacted', color=colors[1])
    ax.set(xticks=positions, xticklabels=labels, xlabel='Batch size (sensor rows)', ylabel='Active Parquet files', title='Compaction file count')
    ax.legend(); ax.grid(axis='y', alpha=0.2)
    save(fig, 'file_counts')

    fig, ax = plt.subplots(figsize=(10, 5))
    sizes, size_labels = [], []
    for entry, label in zip(entries, labels):
        for key, display in [('before','Baseline'),('after','Compacted')]:
            sizes.append([size/1024 for size in entry[key]['file_sizes_bytes']])
            size_labels.append(f'{label}\n{display}')
    ax.boxplot(sizes, tick_labels=size_labels, showfliers=True)
    ax.set(yscale='log', ylabel='Active file size (KiB, log scale)', title='Active file-size distribution (box: IQR, line: median)')
    ax.grid(axis='y', alpha=0.2)
    save(fig, 'file_sizes')

    names = list(entries[0]['queries']['baseline'])
    fig, axes = plt.subplots(1, len(names), figsize=(5*len(names), 5), squeeze=False)
    for name, ax in zip(names, axes[0]):
        for offset, layout, color in [(-0.18,'baseline',colors[0]),(0.18,'compacted',colors[1])]:
            values = [e['queries'][layout][name]['median_seconds']*1000 for e in entries]
            errors = [e['queries'][layout][name]['std_seconds']*1000 for e in entries]
            ax.bar(positions+offset, values, 0.36, yerr=errors, capsize=3, label=layout.capitalize(), color=color)
        ax.set(xticks=positions, xticklabels=labels, xlabel='Batch size (rows)', ylabel='Median latency (ms)', title=name.replace('_',' '))
        ax.legend(); ax.grid(axis='y', alpha=0.2)
    fig.suptitle(f"Warm queries: {metrics['repeats']} repeats; whiskers = SD of repeats", y=1.02)
    save(fig, 'query_latency')

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, mapped['throughput'], color=colors[0])
    ax.bar_label(bars, fmt='%.0f', padding=4)
    ax.margins(y=0.18)
    ax.set(xlabel='Batch size (sensor rows)', ylabel='Input sensor rows / second',
           title='End-to-end ingestion (HTTP + Bronze + P3/reporting + Delta)')
    ax.text(0, -0.22, 'One ingestion trial per batch size; includes MES. Mock run generation excluded.', transform=ax.transAxes, fontsize=9)
    ax.grid(axis='y', alpha=0.2)
    save(fig, 'throughput')

    lines = ['# Measured storage benchmark', '', f"Rows: {metrics['rows']:,}; seed: {metrics['seed']}; query repeats: {metrics['repeats']}.",
             '', 'Source: [metrics.json](metrics.json), [queries.csv](queries.csv), [operations.jsonl](operations.jsonl).',
             '', '| Batch size | Accepted / rejected | Files before / after | End-to-end rows/s | Compaction (s) |', '|---|---|---|---|---|']
    for e in entries:
        lines.append(f"| {e['batch_size']:,} | {e['accepted_sensor_rows']:,} / {e['rejected_sensor_rows']:,} | {e['before']['file_count']} / {e['after']['file_count']} | {e['input_rows_per_second']:.1f} | {e['compaction_seconds']:.3f} |")
    lines += ['', 'S3 retained physical bytes are unknown; active-byte changes do not mean disk was freed.',
              'Warm cache is uncontrolled. Throughput is a single end-to-end trial; P3 time includes reports.',
              'Synthetic one-line workload; no conclusion about business causality or multi-partition pruning.', '']
    for path in paths:
        if path.suffix == '.png':
            lines.extend([f'![{path.stem}](figures/{path.name})', ''])
    (metrics_path.parent / 'summary.md').write_text('\n'.join(lines) + '\n')
    return paths


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Render four measured benchmark charts in PNG/SVG.')
    parser.add_argument('metrics', type=Path)
    render_report(parser.parse_args().metrics)
