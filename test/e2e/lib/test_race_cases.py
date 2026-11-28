"""Selection and no-skip evidence for the separate race gate; no product builds."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location('race_cases', ROOT / 'test/source/race_cases.py')
RACE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RACE)


class RaceCases(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        return target

    def test_case_marks_ignore_examples_and_nested_modules(self):
        path = self.write('selected/race_test.go', '''package fixture
// func TestRaceComment(t *testing.T) {}
/* func TestRaceBlock(t *testing.T) {} */
var example = `func TestRaceExample(t *testing.T) {}`
var quoted = "func TestRaceString(t *testing.T) {}"
func
TestRaceSelected(t *testing.T) {}
func TestOrdinary(t *testing.T) {}
''')
        self.write('unrelated/normal_test.go', 'func TestOrdinary(t *testing.T) {}')
        for directory in ('vendor', 'testdata', '_ignored', '.hidden', 'nested'):
            self.write(directory + '/race_test.go', 'func TestRaceIgnored(t *testing.T) {}')
        self.write('nested/go.mod', 'module nested\n')
        self.assertEqual(RACE.names(path), {'TestRaceSelected'})
        self.assertEqual(RACE.candidates(self.root), ['./selected'])

    def test_go_list_files_control_build_tags_and_architecture_selection(self):
        self.write('race_test.go', 'func TestRaceSelected(t *testing.T) {}')
        self.write('race_windows_test.go', 'func TestRaceWindows(t *testing.T) {}')
        self.write('external_test.go', 'func TestRaceExternal(t *testing.T) {}')
        row = dict(ImportPath='example.test/p', Dir=str(self.root), TestGoFiles=['race_test.go'],
                   XTestGoFiles=['external_test.go'], IgnoredGoFiles=['race_windows_test.go'])
        self.assertEqual(RACE.inventory(json.dumps(row)),
                         {'example.test/p': ['TestRaceExternal', 'TestRaceSelected']})
        row['TestGoFiles'] = row['XTestGoFiles'] = []
        with self.assertRaisesRegex(ValueError, 'no TestRace'):
            RACE.inventory(json.dumps(row))
        row['Incomplete'] = True
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            RACE.inventory(json.dumps(row))

    def events(self):
        return [dict(Package='example.test/p', Test=test, Action=action) for test, action in
                [('TestRaceShared', 'run'), ('TestRaceShared/subcase', 'run'),
                 ('TestRaceShared/subcase', 'pass'), ('TestRaceShared', 'pass'), ('', 'pass')]]

    def test_selected_cases_and_every_started_subcase_must_pass(self):
        selected = {'example.test/p': ['TestRaceShared']}
        RACE.validate(self.events(), selected)
        for mutate in (
            lambda rows: rows[:-1],
            lambda rows: [row for row in rows if row['Test'] != 'TestRaceShared'],
            lambda rows: [dict(row, Action='skip') if row['Test'].endswith('/subcase') and row['Action'] == 'pass' else row for row in rows],
            lambda rows: [dict(row, Action='fail') if row['Test'] == '' else row for row in rows],
            lambda rows: [dict(row, Test='TestOrdinary') if row['Test'] == 'TestRaceShared' else row for row in rows],
            lambda rows: rows + [rows[0]],
            lambda rows: rows + [rows[-1]],
            lambda rows: [dict(row, Package='unselected') for row in rows],
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                RACE.validate(mutate(self.events()), selected)


if __name__ == '__main__':
    unittest.main()
