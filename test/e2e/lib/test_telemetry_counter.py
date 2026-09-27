"""Exercise the actual Proxy metrics parser with Prometheus counter samples."""

from pathlib import Path
import re
import subprocess
import tempfile
import unittest


SOURCE = (Path(__file__).resolve().parents[1] / "cases/telemetry.proxy.sh").read_text()
PARSER = re.search(r"(?ms)^notfound_total\(\) \{\n.*?^\}", SOURCE).group()


class NotfoundCounterTest(unittest.TestCase):
    def parse(self, metrics):
        with tempfile.TemporaryDirectory() as directory:
            sample = Path(directory) / "metrics"
            sample.write_text(metrics)
            return subprocess.run(
                ["bash", "-c", 'set -euo pipefail\n' + PARSER + '\nnotfound_total "$1"', "_", str(sample)],
                text=True, capture_output=True, timeout=5,
            )

    def test_only_notfound_series_contribute(self):
        for sample, expected in (
            ("", 0),
            ('# HELP data_requests_total requests\ndata_requests_total{result="badrequest"} 99\n', 0),
            ('data_requests_total{result="notfound"} 3\n', 3),
            ('data_requests_total{worker="1",result="notfound"} 2\n'
             'data_requests_total{result="notfound",worker="2"} 4 1234\n', 6),
            ('data_requests_total{result="notfound"} 2e1\n', 20),
            ('other_data_requests_total{result="notfound"} 8\n'
             'data_requests_total{other_result="notfound"} 8\n', 0),
        ):
            with self.subTest(sample=sample):
                result = self.parse(sample)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(float(result.stdout), expected)

    def test_invalid_counter_fails_closed(self):
        for value in ("NaN", "+Inf", "-Inf", "-1", "invalid"):
            with self.subTest(value=value):
                result = self.parse('data_requests_total{result="notfound"} ' + value + '\n')
                self.assertNotEqual(result.returncode, 0)

    def test_current_counter_must_exceed_baseline(self):
        comparison = next(line.strip() for line in SOURCE.splitlines()
                          if line.strip().startswith("if python3 -c"))
        script = comparison + '\n exit 0\nelse\n exit 1\nfi\n'
        for before, after, expected in ((0, 0, 1), (5, 5, 1), (5, 4, 1), (5, 6, 0)):
            with self.subTest(before=before, after=after):
                result = subprocess.run(["bash", "-c", f"baseline={before}; current={after}\n" + script], timeout=5)
                self.assertEqual(result.returncode, expected)


if __name__ == "__main__":
    unittest.main()
