#!/bin/bash
ipython -c "import pytest; pytest.main(['.', '-x', '--pdb'])"
# Insert breakpoints with `import pytest; pytest.set_trace()`
