"""本地不可变存储适配器。"""

from gribuki_trade.storage.adversarial_audit import (
    AdversarialAuditIntegrityError,
    SQLiteAdversarialAuditStore,
)
from gribuki_trade.storage.candidate_store import (
    CandidateEventCollisionError,
    CandidateNotFoundError,
    SQLiteCandidateStore,
)
from gribuki_trade.storage.event_store import NewsSourceProbeClaim, SQLiteEventStore
from gribuki_trade.storage.exit_plans import (
    ExitPlanStoreConcurrencyError,
    ExitPlanStoreConflictError,
    ExitPlanStoreError,
    ExitPlanStoreIntegrityError,
    SQLiteExitPlanStore,
)
from gribuki_trade.storage.live_records import (
    LiveConfirmationCommit,
    LiveRecordConflictError,
    LiveRecordIntegrityError,
    LiveRecordStateError,
    LiveRecordStoreError,
    SQLiteLiveRecordStore,
    StoredLiveCommand,
)
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
from gribuki_trade.storage.paper_day import (
    PaperDayStoreConflictError,
    PaperDayStoreError,
    PaperDayStoreIntegrityError,
    PaperDayStoreLeaseError,
    SQLitePaperDayStore,
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
    "AdversarialAuditIntegrityError",
    "ArchivedDailyBarEvidence",
    "BodyNotRetainedError",
    "CandidateEventCollisionError",
    "CandidateNotFoundError",
    "DispatchSummary",
    "ExitPlanStoreConcurrencyError",
    "ExitPlanStoreConflictError",
    "ExitPlanStoreError",
    "ExitPlanStoreIntegrityError",
    "FileRawDocumentStore",
    "InvalidReviewCaseTransitionError",
    "LiveRecordConflictError",
    "LiveConfirmationCommit",
    "LiveRecordIntegrityError",
    "LiveRecordStateError",
    "LiveRecordStoreError",
    "NewsSourceProbeClaim",
    "OutboxDispatcher",
    "OutboxItem",
    "OutboxStatus",
    "PaperLedgerConcurrencyError",
    "PaperLedgerConflictError",
    "PaperLedgerError",
    "PaperLedgerIntegrityError",
    "PaperDayStoreConflictError",
    "PaperDayStoreError",
    "PaperDayStoreIntegrityError",
    "PaperDayStoreLeaseError",
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
    "SQLiteAdversarialAuditStore",
    "SQLiteLiveRecordStore",
    "SQLiteExitPlanStore",
    "SQLiteCandidateStore",
    "SQLiteOutbox",
    "SQLitePaperLedger",
    "SQLitePaperDayStore",
    "SQLitePaperOrderStore",
    "SQLiteResearchStore",
    "SQLiteReviewCaseStore",
    "SQLiteResearchRunStore",
    "SQLiteSourceHealthStore",
    "SQLiteStrategyExperimentStore",
    "SourceHealthCollisionError",
    "SourceHealthSummary",
    "StoredRawDocument",
    "StoredLiveCommand",
    "StoredResearchRun",
    "StoredStrategyExperiment",
    "StrategyExperimentCollisionError",
    "archive_daily_bar_evidence",
    "daily_bar_evidence_canonical_url",
    "research_run_id",
    "research_run_document_sha256",
]
