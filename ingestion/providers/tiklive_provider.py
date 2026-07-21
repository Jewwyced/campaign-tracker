"""
ingestion/tiklive_provider.py — COMPATIBILITY SHIM.

TikLiveAPIProvider moved to ingestion/providers/tiklive.py as part of the
provider-boundary refactor. Re-exported here, unchanged, so existing
imports keep working without modification — notably service.py's
`from .tiklive_provider import TikLiveAPIProvider`, and the diagnostic
scripts' `from ingestion.tiklive_provider import TikLiveAPIProvider`.

Do not add new logic here — this file exists ONLY for backward
compatibility. Anything new belongs in providers/tiklive.py.
"""

from .providers.tiklive import TikLiveAPIProvider

__all__ = ["TikLiveAPIProvider"]