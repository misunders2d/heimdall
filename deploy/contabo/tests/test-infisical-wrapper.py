#!/usr/bin/env python3
"""Synthetic files only: never source host credentials or launch Infisical."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'heimdall-infisical-run.sh'

class WrapperTest(unittest.TestCase):
    def test_project_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / 'base.env'
            machine = root / 'machine.env'
            selector = root / 'selector.env'
            base.write_text('INFISICAL_PROJECT_ID=wrong-base\n')
            machine.write_text('INFISICAL_PROJECT_ID=wrong-machine\nINFISICAL_UNIVERSAL_AUTH_CLIENT_ID=fixture\nINFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET=fixture\n')
            fake = root / 'fake-exec'
            fake.write_text('#!/usr/bin/env bash\n[[ "$INFISICAL_PROJECT_ID" == heimdall-fixture && "$1" == -- && "$2" == probe ]]\n')
            fake.chmod(0o700)
            script = SOURCE.read_text().replace('/etc/pi-infisical-machine.env', str(machine)).replace('/etc/pi-infisical.env', str(base)).replace('/etc/heimdall/infisical.env', str(selector)).replace('/usr/local/lib/pi-infisical-exec.js', str(fake))
            # Ownership check remains production-root-only; synthetic test can run non-root.
            script = script.replace("'0:600'", repr(str(os.getuid()) + ':600'))
            wrapper = root / 'wrapper'
            wrapper.write_text(script)
            def run():
                return subprocess.run(['bash', str(wrapper), 'probe'], capture_output=True, text=True)
            self.assertEqual(run().returncode, 2)  # no Pi fallback
            selector.write_text('INFISICAL_PROJECT_ID=heimdall-fixture\n')
            selector.chmod(0o600)
            result = run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, '')
            selector.chmod(0o644)
            self.assertEqual(run().returncode, 2)
            selector.chmod(0o600)
            selector.write_text('INFISICAL_PROJECT_ID=heimdall-fixture\nEXTRA=value\n')
            self.assertEqual(run().returncode, 2)
            selector.write_text('INFISICAL_PROJECT_ID=\n')
            self.assertEqual(run().returncode, 2)
            selector.unlink()
            selector.symlink_to(machine)
            self.assertEqual(run().returncode, 2)

if __name__ == '__main__':
    unittest.main()
