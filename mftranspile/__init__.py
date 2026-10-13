"""mftranspile — a COBOL -> PySpark conversion app.

Public API:
    from mftranspile import translate, verify
"""
from mftranspile.translate import translate, TranslationResult  # noqa: F401
from mftranspile.verify import verify  # noqa: F401

__all__ = ["translate", "TranslationResult", "verify"]
__version__ = "0.1.0"
