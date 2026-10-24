"""Single Python economic executor; adapters provide explicit services/inputs."""

from .state import ReplayContext
from .hazard import HazardDiagnostics
from .lifecycle import LifecycleAccounting
from .policy_setup import PolicySetup
from .inventory_policy import InventoryPolicy
from .cooldown_policy import CooldownPolicy
from .latency import LatencyAdapter
from .book import BookAdapter
from .journal import JournalAdapter
from .risk import RiskAdapter
from .orders import OrderState
from .fill_policy import FillPolicy
from .exchange import ExchangeAdapter
from .action_policy import ActionPolicy
from .rest_gateway import RestGatewayAdapter
from .quote_cycle import QuoteCycle
from .emergency import EmergencyActions
from .restoration import OrderRestoration
from .fills import FillExecution
from .cursor import ReplayCursor
from .initialization import ReplayInitialization
from .events import ReplayEvents
from .finalization import ReplayFinalization


class ReplayExecutor(
    HazardDiagnostics,
    LifecycleAccounting,
    PolicySetup,
    InventoryPolicy,
    CooldownPolicy,
    LatencyAdapter,
    BookAdapter,
    JournalAdapter,
    RiskAdapter,
    OrderState,
    FillPolicy,
    ExchangeAdapter,
    ActionPolicy,
    RestGatewayAdapter,
    QuoteCycle,
    EmergencyActions,
    OrderRestoration,
    FillExecution,
    ReplayCursor,
    ReplayInitialization,
    ReplayEvents,
    ReplayFinalization,
):
    def __init__(self, *, services, **inputs):
        self.services = services
        self.context = ReplayContext(**inputs)
        self.state = None

    def run(self):
        self.initialize()
        checkpoint = self.run_events()
        if checkpoint is not None:
            return checkpoint
        return self.finalize()
