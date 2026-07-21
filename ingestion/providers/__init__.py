"""
ingestion/providers/__init__.py — compatibility layer for the
provider-boundary refactor.

The old ingestion/providers.py was a single flat file. It's now this
package (base.py + tikapi.py + tiklive.py), but every name it used to
export is re-exported here under the same names — so
`from ingestion.providers import default_provider` (and every other
existing import of this module) keeps working completely unchanged.
"""

from .base import BaseProvider, FallbackProvider, ProviderPipeline
from .tikapi import TikAPIProvider
from .tiklive import TikLiveAPIProvider

# ── Default pipeline instance ─────────────────────────────────────────────────
# Discovery/qualification/ingestion code imports and uses this. To add a
# new provider (Apify, Bright Data, etc.), add it to this list — nothing
# else in the codebase changes.

default_provider = ProviderPipeline([
    TikLiveAPIProvider(),
    TikAPIProvider(),

    # FallbackProvider(),  ← uncomment when a real fallback exists
])