#!/usr/bin/env python3
"""Run explicitly marked TestRace cases without compiling unrelated packages.

Normal tests use -skip=^TestRace; this entry uses -race -run=^TestRace. The
prefix is the per-case opt-in, with a comment beside each case explaining the
shared-memory concurrency being checked. Discovery is source-only, followed by
go list for just the candidate packages to honor the actual Go build context.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
FILTER = '^TestRace'
# Ignore examples in comments and strings; allow ordinary Go whitespace between
# declaration tokens. Compilation remains Go's responsibility, not this scanner's.
NONCODE = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|`[^`]*`', re.S)
CASE = re.compile(r'\bfunc\s+(TestRace\w*)\s*\(')


def names(path):
    return set(CASE.findall(NONCODE.sub(' ', path.read_text())))


def candidates(root):
    selected = []
    for directory, children, files in os.walk(root):
        children[:] = sorted(name for name in children if not name.startswith(('.', '_'))
                             and name not in ('vendor', 'testdata')
                             and not (Path(directory) / name / 'go.mod').exists())
        if any(names(Path(directory) / name) for name in files if name.endswith('_test.go')):
            relative = Path(directory).relative_to(root).as_posix()
            selected.append('.' if relative == '.' else './' + relative)
    return sorted(selected)


def inventory(text):
    decoder, selected = json.JSONDecoder(), {}
    while text.strip():
        row, end = decoder.raw_decode(text.lstrip())
        text = text.lstrip()[end:]
        if row.get('Error') or row.get('DepsErrors') or row.get('Incomplete'):
            raise ValueError('incomplete race package: ' + str(row.get('ImportPath')))
        tests = set()
        for name in row.get('TestGoFiles', []) + row.get('XTestGoFiles', []):
            tests.update(names(Path(row['Dir']) / name))
        if tests:
            package = row['ImportPath']
            if package in selected:
                raise ValueError('duplicate race package: ' + package)
            selected[package] = sorted(tests)
    if not selected:
        raise ValueError('no TestRace cases in the selected Go build context')
    return dict(sorted(selected.items()))


def validate(events, selected):
    expected = {(package, test) for package, tests in selected.items() for test in tests}
    started, finished, packages = set(), set(), set()
    for event in events:
        if event.get('Action') == 'build-output':
            continue
        package, test, action = event.get('Package'), event.get('Test'), event.get('Action')
        if package not in selected:
            raise ValueError('unexpected race package: ' + str(package))
        if test:
            if (package, test.split('/')[0]) not in expected:
                raise ValueError('unmarked case executed: ' + test)
            key = (package, test)
            if action == 'run':
                if key in started:
                    raise ValueError('race case executed twice: ' + test)
                started.add(key)
            elif action in ('pass', 'fail', 'skip'):
                if action != 'pass' or key not in started or key in finished:
                    raise ValueError('race case failed, skipped, or duplicated: ' + test)
                finished.add(key)
        elif action in ('pass', 'fail', 'skip'):
            if action != 'pass' or package in packages:
                raise ValueError('race package failed, skipped, or duplicated: ' + package)
            packages.add(package)
    if not expected <= finished or started != finished or packages != set(selected):
        raise ValueError('missing race case, subcase, or package result')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list', action='store_true', help='show selected cases without compiling tests')
    args = parser.parse_args(argv)
    paths = candidates(ROOT)
    if not paths:
        raise ValueError('no explicitly marked TestRace cases')
    env = dict(os.environ, CGO_ENABLED='1')
    selected = inventory(subprocess.check_output(['go', 'list', '-race', '-json', *paths],
                                                cwd=ROOT, env=env, text=True))
    print(json.dumps({'race_cases': selected}, sort_keys=True), flush=True)
    if args.list:
        return 0
    # vet has its own required gate. A single invocation shares the instrumented
    # dependency build cache; unrelated package tests are never compiled here.
    command = ['go', 'test', '-json', '-race', '-vet=off', '-p=1', '-count=1',
               '-timeout=5m', '-run=' + FILTER, *selected]
    events = []
    with subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            print(line, end='', flush=True)
            events.append(json.loads(line))
        code = process.wait()
    if code:
        return code
    validate(events, selected)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print('race selection: ' + str(error), file=sys.stderr)
        raise SystemExit(1)
