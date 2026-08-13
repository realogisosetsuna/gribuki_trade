"""Local immutable storage adapters."""

from gribuki_trade.storage.candidate_store import (
    CandidateEventCollisionError,
    CandidateNotFoundError,
    SQLiteCandidateStore,
)
from gribuki_trade.storage.event_store import SQLiteEventStore
from gribuki_trade.storage.market_evidence import (
    ArchivedDailyBarEvidence,
    archive_daily_bar_evidence,
    daily_bar_evidence_canonical_url,
)
from gribuki_trade.storage.outbox import (
    DispatchSummary,
    OutboxDispatcher,
    OutboxItem,
    OutboxStatus,
    SQLiteOutbox,
)
from gribuki_trade.storage.paper_ledger import (
    PaperLedgerConcurrencyError,
    PaperLedgerConflictError,
    PaperLedgerError,
    PaperLedgerIntegrityError,
    SQLitePaperLedger,
)
from gribuki_trade.storage.paper_orders import (
    PaperOrderEvent,
    PaperOrderEventType,
    PaperOrderStoreConflictError,
    PaperOrderStoreError,
    PaperOrderStoreIntegrityError,
    PaperOrderStoreLeaseError,
    PaperRunRecord,
    SQLitePaperOrderStore,
)
from gribuki_trade.storage.raw_store import (
    BodyNotRetainedError,
    FileRawDocumentStore,
    StoredRawDocument,
)
from gribuki_trade.storage.research_runs import (
    ResearchRunCollisionError,
    SQLiteResearchRunStore,
    StoredResearchRun,
    research_run_document_sha256,
    research_run_id,
)
from gribuki_trade.storage.research_store import (
    ResearchRecordCollisionError,
    SQLiteResearchStore,
)
from gribuki_trade.storage.review_case_store import (
    InvalidReviewCaseTransitionError,
    ReviewCaseEventCollisionError,
    ReviewCaseNotFoundError,
    SQLiteReviewCaseStore,
)
from gribuki_trade.storage.source_health import (
    ProviderRun,
    ProviderRunStatus,
    SourceHealthCollisionError,
    SourceHealthSummary,
    SQLiteSourceHealthStore,
)
from gribuki_trade.storage.strategy_experiments import (
    SQLiteStrategyExperimentStore,
    StoredStrategyExperiment,
    StrategyExperimentCollisionError,
)

__all__ = [
    "ArchivedDailyBarEvidence",
    "BodyNotRetainedError",
    "CandidateEventCollisionError",
    "CandidateNotFoundError",
    "DispatchSummary",
    "FileRawDocumentStore",
    "InvalidReviewCaseTransitionError",
    "OutboxDispatcher",
    "OutboxItem",
    "OutboxStatus",
    "PaperLedgerConcurrencyError",
    "PaperLedgerConflictError",
    "PaperLedgerError",
    "PaperLedgerIntegrityError",
    "PaperOrderEvent",
    "PaperOrderEventType",
    "PaperOrderStoreConflictError",
    "PaperOrderStoreError",
    "PaperOrderStoreIntegrityError",
    "PaperOrderStoreLeaseError",
    "PaperRunRecord",
    "ProviderRun",
    "ProviderRunStatus",
    "ResearchRecordCollisionError",
    "ResearchRunCollisionError",
    "ReviewCaseEventCollisionError",
    "ReviewCaseNotFoundError",
    "SQLiteEventStore",
    "SQLiteCandidateStore",
    "SQLiteOutbox",
    "SQLitePaperLedger",
    "SQLitePaperOrderStore",
    "SQLiteResearchStore",
    "SQLiteReviewCaseStore",
    "SQLiteResearchRunStore",
    "SQLiteSourceHealthStore",
    "SQLiteStrategyExperimentStore",
    "SourceHealthCollisionError",
    "SourceHealthSummary",
    "StoredRawDocument",
    "StoredResearchRun",
    "StoredStrategyExperiment",
    "StrategyExperimentCollisionError",
    "archive_daily_bar_evidence",
    "daily_bar_evidence_canonical_url",
    "research_run_id",
    "research_run_document_sha256",
]
