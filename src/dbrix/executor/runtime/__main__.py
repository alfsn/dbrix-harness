"""PID 1 of the executor container: python -m dbrix.executor.runtime <catalog> <preview_rows> <max_rows>"""

import sys

from dbrix.executor.runtime import serve

serve(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
