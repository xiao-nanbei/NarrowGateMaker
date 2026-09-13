// Compile the actual production implementation, not a copied audit excerpt.
// The full extension has separate Python integration/parity tests.
#include "../narrowgate_cpp/tick_replay.cpp"

#include <cassert>
#include <iostream>

int main() {
    using namespace narrowgate_cpp;
    const std::vector<std::int64_t> timestamps{0, 1000, 10000, 100000};
    const std::vector<double> prices{100.0, 101.0, 102.0, 103.0};
    TickReplayInput input;
    input.trade_ts_ms = timestamps;
    input.trade_price = prices;
    const auto exact = markout_at_horizon(input, 0, 1000, 100.0, Side::Buy);
    assert(exact.value == 1.0 && exact.status == "exact");
    assert(exact.actual_end_ms == 1000 && exact.fixed_horizon_valid);
    const auto sell = markout_at_horizon(input, 0, 1000, 100.0, Side::Sell);
    assert(sell.value == -1.0);
    const auto delayed = markout_at_horizon(input, 10000, 1000, 100.0, Side::Buy);
    assert(delayed.value == 3.0 && delayed.status == "delayed");
    assert(delayed.requested_end_ms == 11000 && delayed.actual_end_ms == 100000);
    assert(delayed.gap_ms == 89000 && !delayed.fixed_horizon_valid);
    const auto tail = markout_at_horizon(input, 100000, 1000, 100.0, Side::Buy);
    assert(std::isnan(tail.value) && tail.status == "censored");
    assert(!tail.actual_end_ms && tail.censor_reason == "no_future_trade");
    TickReplayInput empty;
    assert(std::isnan(markout_at_horizon(empty, 0, 1000, 100.0, Side::Buy).value));

    TickReplayResult result;
    ReplayOrder order;
    order.price = 100.0;
    order.side = Side::Buy;
    append_fill_trace(result, input, order, 100000, 103.0, 0.0, 0.002,
                      0.001, 0.0, 0.001, 0.0001, 1000, 100);
    assert(result.fill_trace.size() == 1);
    const auto& row = result.fill_trace.front();
    assert(std::isnan(row.markout_1s) && std::isnan(row.ev_1s));
    assert(!row.toxic_1s.has_value());
    assert(row.fill_qty == 0.001 && row.fill_fee_usdc == 0.00001);
    assert(row.inventory_after_fill == 0.001);
    std::cout << "native trace markout: exact, delayed, tail, empty, signs, unknown, fill facts PASS\n";
}
