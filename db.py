"""
db.py — single source of truth for the database connection.

Every blueprint imports `db` from here instead of redefining its own
connection logic, so there's exactly one place that knows how to talk to Neon.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "")

def db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)