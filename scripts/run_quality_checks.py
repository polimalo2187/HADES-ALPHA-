from __future__ import annotations

import compileall
import sys
import unittest
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    app_ok = compileall.compile_dir(repo_root / 'app', quiet=1, force=True)
    tests_ok = compileall.compile_dir(repo_root / 'tests', quiet=1, force=True)
    if not (app_ok and tests_ok):
        print('❌ Falló la compilación de app/tests')
        return 1

    suite = unittest.defaultTestLoader.discover(str(repo_root / 'tests'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        print('❌ Fallaron los tests')
        return 1

    print('✅ Quality checks OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
