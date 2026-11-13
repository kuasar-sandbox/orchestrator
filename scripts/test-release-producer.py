"""Execute the dependency bindings from the shipping workflow for both producer layouts."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

WORKFLOW = (Path(__file__).resolve().parents[1] / ".github/workflows/component-release.yml").read_text()
BASE = ['accelerator', 'connector', 'sandboxer']
LEGACY_PACKAGER = "9de9b10d6e8227a2c88042376495cc038b0f47f4"


def step(name):
    return WORKFLOW.split("      - name: " + name + "\n", 1)[1].split("      - name:", 1)[0]


class ProducerTests(unittest.TestCase):
    def test_old_and_new_inputs(self):
        self.assertRegex(WORKFLOW, r"(?m)^      runtime_version:\n        description: .*\n        required: false$")
        self.assertIn("ref: ${{ needs.preflight.outputs.runtime_version && github.workflow_sha || '" +
                      LEGACY_PACKAGER + "' }}", step("Check out trusted packaging tools"))
        self.assertIn("persist-credentials: false", step("Check out trusted packaging tools"))
        self.assertIn('ref: ${{ needs.preflight.outputs.framework_sha }}',
                      WORKFLOW.split("\n  publish:\n", 1)[1])
        freeze = step("Freeze admitted source and selected dependency tags").split("run: |", 1)[1]
        restore = step("Restore fixed producer inputs").split("run: |", 1)[1]
        # Bash expands the argument array independently in each Workbench command.
        workspaces = re.findall(r"runtime=\(\)\n\s+if \[\[ -n '[^']*runtime_version[^']*' \]\]; then[\s\S]*?export GOWORK", WORKFLOW)
        self.assertEqual(len(workspaces), 1)
        for has_runtime in (False, True):
            with self.subTest(runtime=has_runtime), tempfile.TemporaryDirectory() as directory:
                deps = BASE + (["runtime"] if has_runtime else [])
                outputs = dict(version="v1.2.3", source_sha="a"*40)
                env = dict(os.environ, SOURCE_REF="main", SOURCE_SHA=outputs["source_sha"])
                for i, unit in enumerate(BASE + ["runtime"]):
                    present = unit in deps
                    tag = ("runtime-" if unit == "runtime" else "") + "v1.2.2" if present else ""
                    sha = str(i+1)*40 if present else ""
                    outputs.update({unit+"_version": tag, unit+"_sha": sha})
                    env.update({unit.upper()+"_TAG": tag, unit.upper()+"_VERSION": tag, unit.upper()+"_SHA": sha})
                for body in (freeze, restore, *workspaces):
                    command = re.sub(r"\$\{\{ (?:needs.preflight|steps.request).outputs.(\w+) \}\}",
                                     lambda match: outputs[match[1]], body)
                    record = Path(directory)/"argv"
                    env["RECORD"] = str(record)
                    # Intercept only the tool boundary; the GitHub workflow's
                    # actual Bash prepares the source, release, and tag/SHA arguments.
                    shell = 'python3() { printf "%s\\0" "$@" > "$RECORD"; }\n' + command
                    subprocess.run(["bash", "-euo", "pipefail", "-c", shell], env=env, check=True)
                    argv = record.read_bytes().decode().split("\0")
                    expected = [(("guest-runtime" if unit == "runtime" and body in workspaces else unit),
                                 outputs[unit+"_version"], outputs[unit+"_sha"]) for unit in deps]
                    self.assertEqual([tuple(argv[i+1:i+4]) for i, arg in enumerate(argv) if arg == "--dependency"], expected)
                    if body == freeze:
                        self.assertEqual(argv[argv.index("--admission")+1], "producer-source.tar")


if __name__ == "__main__":
    unittest.main()
