#!/usr/bin/env python3
"""Release jobs consume the environment CLI without installing or selecting tools."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class EnvironmentTools(unittest.TestCase):
    def cli_steps(self):
        steps = []
        for workflow in (ROOT / ".github/workflows").glob("*.yml"):
            lines = workflow.read_text().splitlines()
            for index, line in enumerate(lines):
                if line.strip() != "- name: Check environment GitHub CLI":
                    continue
                self.assertEqual(lines[index + 1].strip(), "run: |")
                prefix = line[:len(line) - len(line.lstrip())] + "    "
                command = []
                for candidate in lines[index + 2:]:
                    if not candidate.startswith(prefix):
                        break
                    command.append(candidate[len(prefix):])
                steps.append("\n".join(command))
        self.assertTrue(steps, "release jobs must check their environment CLI")
        return set(steps)

    def test_cli_accepts_environment_versions_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "independent tools"
            tools.mkdir()
            cli = tools / "gh"
            for version in ("distro-build", "different-compatible-release"):
                cli.write_text("#!/bin/sh\n"
                               'if [ "$1:${2-}" = api:--help ]; then echo "  --slurp  collect pages"; exit 0; fi\n'
                               "echo " + version + "\n"
                               'printf "%s:%s\\n" "${GOTOOLCHAIN+x}" "${GOTOOLCHAIN-}" > "$OBSERVED"\n')
                cli.chmod(0o755)
                before = cli.read_bytes()
                for policy in (None, "local", "auto", "go1.99.1+path"):
                    env = dict(os.environ, PATH=str(tools), OBSERVED=str(root / "observed"),
                               GITHUB_PATH=str(root / "github-path"))
                    if policy is None:
                        env.pop("GOTOOLCHAIN", None)
                    else:
                        env["GOTOOLCHAIN"] = policy
                    for command in self.cli_steps():
                        result = subprocess.run([shutil.which("bash"), "-e", "-c", command],
                                                env=env, text=True, capture_output=True, timeout=10)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(result.stdout.strip(), version)
                    self.assertEqual((root / "observed").read_text().strip(),
                                     ":" if policy is None else "x:" + policy)
                    self.assertEqual(cli.read_bytes(), before)
                    self.assertFalse((root / "github-path").exists())

    def test_missing_cli_fails_without_installing(self):
        with tempfile.TemporaryDirectory() as directory:
            for command in self.cli_steps():
                result = subprocess.run([shutil.which("bash"), "-e", "-c", command],
                                        env=dict(os.environ, PATH=directory),
                                        text=True, capture_output=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("gh", result.stderr)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_incompatible_cli_fails_before_any_remote_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cli = root / "gh"
            cli.write_text('#!/bin/sh\n'
                           'case "$1:${2-}" in --version:) echo old-distro-gh;; '
                           'api:--help) echo "  --paginate";; '
                           '*) echo REMOTE_OPERATION; exit 77;; esac\n')
            cli.chmod(0o755)
            for command in self.cli_steps():
                result = subprocess.run([shutil.which("bash"), "-e", "-c", command],
                                        env=dict(os.environ, PATH=directory),
                                        text=True, capture_output=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must support api --slurp", result.stderr)
                self.assertNotIn("REMOTE_OPERATION", result.stdout)

    def test_release_jobs_do_not_pin_toolchain_policy_or_install_cli(self):
        for workflow in (ROOT / ".github/workflows").glob("*.yml"):
            source = workflow.read_text()
            self.assertNotIn("install-gh-cli.sh", source)
            self.assertNotIn("GOTOOLCHAIN: local", source)
        for directory in ("scripts", "release"):
            self.assertFalse((ROOT / directory / "install-gh-cli.sh").exists())

    def test_workbench_package_receives_only_explicit_dependency_versions(self):
        lines = (ROOT / ".github/workflows/component-release.yml").read_text().splitlines()
        start = next(i for i, line in enumerate(lines)
                     if line.strip() == "- name: Build, test and package orchestrator")
        run = next(i for i in range(start, len(lines)) if lines[i].strip() == "run: |")
        prefix = " " * (len(lines[run]) - len(lines[run].lstrip()) + 2)
        command = []
        for line in lines[run + 1:]:
            if not line.startswith(prefix):
                break
            command.append(line[len(prefix):])
        values = {"source_sha": "a" * 40, "version": "v1.2.3-preview.20261009.4",
                  "accelerator_version": "v2.3.4", "connector_version": "v3.4.5-preview.20261008.2",
                  "sandboxer_version": "v4.5.6"}
        program = re.sub(r"\$\{\{ needs\.preflight\.outputs\.([a-z_]+) \}\}",
                         lambda match: values[match.group(1)], "\n".join(command))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "tools"
            scripts = root / "orchestrator/scripts"
            tools.mkdir()
            scripts.mkdir(parents=True)
            commands = {
                "go": "#!/bin/sh\nexit 0\n", "make": "#!/bin/sh\nexit 0\n",
                "git": "#!/bin/sh\ncase $1 in rev-parse) echo " + values["source_sha"]
                       + ";; show) echo 1;; *) exit 92;; esac\n",
            }
            for name, contents in commands.items():
                path = tools / name
                path.write_text(contents)
                path.chmod(0o755)
            (scripts / "release.sh").write_text(
                'set -eu\nprintf "%s|%s|%s|%s|%s|%s\\n" "$1" "$2" "$3" '
                '"$ACCELERATOR_VERSION" "$CONNECTOR_VERSION" "$SANDBOXER_VERSION" >> "$OBSERVED"\n')
            observed = root / "observed"
            for arch in ("x86_64", "aarch64"):
                with self.subTest(arch=arch):
                    observed.unlink(missing_ok=True)
                    # Workbench does not inherit the release job's host env.
                    environment = {"PATH": str(tools) + os.pathsep + os.environ["PATH"],
                                   "HOME": str(root), "TARGET_ARCH": arch, "OBSERVED": str(observed)}
                    result = subprocess.run([shutil.which("bash"), "-euo", "pipefail", "-c", program],
                                            cwd=root, env=environment, text=True, capture_output=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(observed.read_text().splitlines(), [
                        "|".join((operation, values["version"], arch, values["accelerator_version"],
                                  values["connector_version"], values["sandboxer_version"]))
                        for operation in ("package", "validate")])


if __name__ == "__main__":
    unittest.main()
