#pragma once

#include <cmath>
#include <cstdint>
#include <string>

#include "common.hpp"

namespace narrowgate_cpp {

inline constexpr std::uint16_t kTransportContractAbiVersion = 1;
inline constexpr const char* kTransportContractSchemaVersion =
    "narrowgate_cpp_transport_contract.v1";

// The first transport-contract phase is intentionally restricted to the
// current exchange product. Adding another product is an ABI change, not a
// runtime string substitution.
enum class TransportProduct : std::uint8_t {
    UsdMFutures = 0,
};

enum class TransportBackendKind : std::uint8_t {
    Unspecified = 0,
    PythonUsdmLegacy = 1,
    CppUsdmWebSocket = 2,
    CppUsdmRest = 3,
    CppUsdmFix = 4,
};

enum class CanonicalEventKind : std::uint8_t {
    Unspecified = 0,
    MarketTrade = 1,
    BookTicker = 2,
    DepthDelta = 3,
    OrderUpdate = 4,
    AccountUpdate = 5,
    SessionState = 6,
};

enum class CanonicalOrderType : std::uint8_t {
    Unspecified = 0,
    Limit = 1,
    Market = 2,
};

enum class CanonicalSide : std::uint8_t {
    Unspecified = 0,
    Buy = 1,
    Sell = 2,
};

enum class CanonicalTimeInForce : std::uint8_t {
    Unspecified = 0,
    Gtx = 1,
    Ioc = 2,
};

// A local request progresses through these phases. In particular,
// LocalValidated/Enqueued never imply that the exchange accepted the order.
enum class TransportPhase : std::uint8_t {
    Unspecified = 0,
    LocalValidated = 1,
    Enqueued = 2,
    WireDispatched = 3,
    ExchangeAckAccepted = 4,
    ExchangeAckRejected = 5,
    ExchangeUpdate = 6,
    ExchangeTerminal = 7,
};

// Ambiguous writes must never be converted into a positive reject or retried
// through a second backend. They remain unknown until exact reconciliation.
enum class TransportUnknownState : std::uint8_t {
    None = 0,
    ConfirmedNotDispatched = 1,
    MayHaveBeenDispatched = 2,
    AwaitingReconciliation = 3,
};

struct CanonicalEventHeader {
    std::uint16_t abi_version = kTransportContractAbiVersion;
    TransportProduct product = TransportProduct::UsdMFutures;
    TransportBackendKind backend = TransportBackendKind::Unspecified;
    CanonicalEventKind event_kind = CanonicalEventKind::Unspecified;
    std::string venue = "BINANCE";
    std::string symbol;
    std::string session_id;
    std::string correlation_id;
    std::uint64_t generation = 0;
    // Exchange event time is a venue clock. Receive and feature-ready times
    // share the live host's monotonic clock and may be ordered with each
    // other; neither may be numerically ordered against exchange event time.
    std::uint64_t exchange_event_time_ns = 0;
    std::uint64_t local_receive_time_ns = 0;
    std::uint64_t feature_ready_time_ns = 0;
    std::uint64_t source_sequence = 0;
    std::uint64_t ingress_sequence = 0;
    bool snapshot = false;
    bool reconciled = false;
};

struct CanonicalOrderIntent {
    std::uint16_t abi_version = kTransportContractAbiVersion;
    TransportProduct product = TransportProduct::UsdMFutures;
    std::string request_id;
    std::string decision_id;
    std::string client_order_id;
    std::string symbol;
    CanonicalSide side = CanonicalSide::Unspecified;
    CanonicalOrderType order_type = CanonicalOrderType::Unspecified;
    CanonicalTimeInForce time_in_force = CanonicalTimeInForce::Unspecified;
    double price = 0.0;
    double quantity = 0.0;
    bool reduce_only = false;
    bool post_only = false;
    std::uint64_t recv_window_ms = 0;
    // Local monotonic deadline used only by the host-side gateway scheduler.
    std::uint64_t deadline_time_ns = 0;
    std::uint64_t expected_ownership_generation = 0;

    [[nodiscard]] const char* validation_error() const noexcept {
        if (abi_version != kTransportContractAbiVersion) {
            return "unsupported transport ABI version";
        }
        if (product != TransportProduct::UsdMFutures) {
            return "transport product must be USD-M Futures";
        }
        if (request_id.empty()) {
            return "request_id is required";
        }
        if (client_order_id.empty()) {
            return "client_order_id is required";
        }
        if (symbol.empty()) {
            return "symbol is required";
        }
        if (side != CanonicalSide::Buy && side != CanonicalSide::Sell) {
            return "side must be BUY or SELL";
        }
        if (!std::isfinite(quantity) || quantity <= 0.0) {
            return "quantity must be finite and positive";
        }
        if (order_type == CanonicalOrderType::Limit) {
            if (!std::isfinite(price) || price <= 0.0) {
                return "limit price must be finite and positive";
            }
            if (time_in_force != CanonicalTimeInForce::Gtx &&
                time_in_force != CanonicalTimeInForce::Ioc) {
                return "limit time_in_force must be GTX or IOC";
            }
        } else if (order_type == CanonicalOrderType::Market) {
            if (price != 0.0) {
                return "market price must be zero";
            }
            if (time_in_force != CanonicalTimeInForce::Unspecified) {
                return "market time_in_force must be unspecified";
            }
        } else {
            return "order_type is required";
        }
        if (post_only &&
            (order_type != CanonicalOrderType::Limit ||
             time_in_force != CanonicalTimeInForce::Gtx)) {
            return "post_only requires a GTX limit order";
        }
        return "";
    }

    [[nodiscard]] bool is_structurally_valid() const noexcept {
        return validation_error()[0] == '\0';
    }
};

struct CanonicalCancelIntent {
    std::uint16_t abi_version = kTransportContractAbiVersion;
    TransportProduct product = TransportProduct::UsdMFutures;
    std::string request_id;
    std::string decision_id;
    std::string client_order_id;
    std::uint64_t exchange_order_id = 0;
    std::string symbol;
    std::string reason;
    std::uint64_t expected_ownership_generation = 0;

    [[nodiscard]] const char* validation_error() const noexcept {
        if (abi_version != kTransportContractAbiVersion) {
            return "unsupported transport ABI version";
        }
        if (product != TransportProduct::UsdMFutures) {
            return "transport product must be USD-M Futures";
        }
        if (request_id.empty()) {
            return "request_id is required";
        }
        if (symbol.empty()) {
            return "symbol is required";
        }
        if (client_order_id.empty() && exchange_order_id == 0) {
            return "client_order_id or exchange_order_id is required";
        }
        return "";
    }

    [[nodiscard]] bool is_structurally_valid() const noexcept {
        return validation_error()[0] == '\0';
    }
};

struct CanonicalCancelAllIntent {
    std::uint16_t abi_version = kTransportContractAbiVersion;
    TransportProduct product = TransportProduct::UsdMFutures;
    std::string request_id;
    std::string decision_id;
    std::string symbol;
    std::string reason;
    std::uint64_t expected_ownership_generation = 0;

    [[nodiscard]] const char* validation_error() const noexcept {
        if (abi_version != kTransportContractAbiVersion) {
            return "unsupported transport ABI version";
        }
        if (product != TransportProduct::UsdMFutures) {
            return "transport product must be USD-M Futures";
        }
        if (request_id.empty()) {
            return "request_id is required";
        }
        if (symbol.empty()) {
            return "symbol is required";
        }
        return "";
    }

    [[nodiscard]] bool is_structurally_valid() const noexcept {
        return validation_error()[0] == '\0';
    }
};

struct TransportReceipt {
    std::uint16_t abi_version = kTransportContractAbiVersion;
    std::string request_id;
    TransportBackendKind backend = TransportBackendKind::Unspecified;
    TransportPhase phase = TransportPhase::Unspecified;
    TransportUnknownState unknown_state = TransportUnknownState::None;
    std::uint64_t generation = 0;
    // `local_time_ns` is from the live host's monotonic clock.
    // `exchange_time_ns` is an external venue clock.  Each domain may be
    // checked for progress against an earlier value from that same domain,
    // but the two numeric values must never be ordered or subtracted without
    // an explicitly measured clock-offset model.
    std::uint64_t local_time_ns = 0;
    std::uint64_t exchange_time_ns = 0;
    std::string reason;

    [[nodiscard]] bool allows_cross_backend_retry() const noexcept {
        return unknown_state == TransportUnknownState::ConfirmedNotDispatched &&
               static_cast<std::uint8_t>(phase) <
                   static_cast<std::uint8_t>(TransportPhase::WireDispatched);
    }
};

// No C++ network backend is implemented or enabled by this contract-only
// phase. The legacy Python USD-M transport remains the sole available
// authority. In particular, the Spot FIX contract must not be reused here.
[[nodiscard]] constexpr bool transport_backend_available(
    TransportBackendKind backend
) noexcept {
    return backend == TransportBackendKind::PythonUsdmLegacy;
}

[[nodiscard]] constexpr const char* transport_backend_unavailable_reason(
    TransportBackendKind backend
) noexcept {
    switch (backend) {
        case TransportBackendKind::CppUsdmWebSocket:
            return "C++ USD-M WebSocket backend is not implemented";
        case TransportBackendKind::CppUsdmRest:
            return "C++ USD-M REST backend is not implemented";
        case TransportBackendKind::CppUsdmFix:
            return "Binance USD-M Futures FIX is unavailable: Binance FIX is "
                   "Spot-only and no official USD-M Futures FIX endpoint exists";
        case TransportBackendKind::Unspecified:
            return "transport backend is unspecified";
        case TransportBackendKind::PythonUsdmLegacy:
            return "";
    }
    return "unknown transport backend";
}

}  // namespace narrowgate_cpp
