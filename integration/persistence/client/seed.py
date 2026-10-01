"""Install the original released SQLite layout, never a renamed fresh layout."""
import sqlite3, sys
from pathlib import Path
root=Path(__file__).resolve().parents[3]
con=sqlite3.connect(sys.argv[1])
con.executescript((root/'crates/sqlite/tests/fixtures/v02-framework.sql').read_text())
con.executescript((root/'crates/sqlite/tests/fixtures/sqlite-state.sql').read_text())
con.commit()
con.close()
