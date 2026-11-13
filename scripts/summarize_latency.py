"""Pool per-method latency samples from repeated TestRPCLocalLoad runs."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    files = sorted(args.directory.glob('run-*.json'))
    if not files:
        raise SystemExit('No run-*.json files found')
    runs = [json.loads(path.read_text(encoding='utf-8')) for path in files]
    samples = {'Cache.Get': [], 'Cache.Set': []}
    trials = []
    for path, run in zip(files, runs):
        if run['failures']:
            raise SystemExit(f'{path}: failed calls must be investigated before publishing')
        trials.append({'file': path.name, 'timestamp_utc': run['timestamp_utc'],
                       'requests': run['requests'], 'failures': run['failures'],
                       'elapsed_seconds': run['elapsed_seconds'],
                       'by_method': {method: {key: stats[key] for key in
                                     ('requests', 'median_ms', 'p95_ms')}
                                     for method, stats in run['by_method'].items()}})
        for method in samples:
            values = run['by_method'][method]['latencies_ms_sorted']
            assert len(values) == run['by_method'][method]['requests']
            samples[method].extend(values)
    def stats(values):
        ordered = sorted(values)
        return {'requests': len(ordered), 'median_ms': statistics.median(ordered),
                'p95_ms': ordered[math.ceil(.95 * len(ordered)) - 1]}
    result = {
        'trials': len(runs), 'requests': sum(r['requests'] for r in runs),
        'failures': sum(r['failures'] for r in runs),
        'by_method': {method: stats(values) for method, values in samples.items()},
        'combined': stats(samples['Cache.Get'] + samples['Cache.Set']),
        'scope': runs[0]['scope'], 'toolchain': runs[0]['go'], 'os': runs[0]['os'],
        'aggregation': 'Pool all samples across all trials; median averages the middle pair; P95 is nearest rank. Not an average of trial percentiles.',
        'runs': trials,
    }
    (args.directory / 'summary.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    root = Path(__file__).resolve().parents[1]
    paths = sorted(root.glob('multicast/*.go')) + sorted(root.glob('rpcache/*.go'))
    hashes = {str(path.relative_to(root)).replace('\\', '/'): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    (args.directory / 'source-hashes.json').write_text(json.dumps(hashes, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({key: result[key] for key in ('trials', 'requests', 'failures', 'by_method', 'combined')}))


if __name__ == '__main__':
    main()
