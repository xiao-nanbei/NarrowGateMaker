// Execution-v1 frame aggregation. Decimal arithmetic deliberately uses CPython's
// native decimal implementation: converting to double before the model boundary
// would change frozen feature semantics. Transport/window state stays shared.
#include <cstdint>
#include <string>
#include <vector>
#include <pybind11/pybind11.h>
#include "binding_registry.hpp"

namespace py = pybind11;
namespace narrowgate_cpp {
namespace {
py::object checked(PyObject* value) {
    if (!value) throw py::error_already_set();
    return py::reinterpret_steal<py::object>(value);
}
py::object add(py::handle a, py::handle b) { return checked(PyNumber_Add(a.ptr(), b.ptr())); }
py::object sub(py::handle a, py::handle b) { return checked(PyNumber_Subtract(a.ptr(), b.ptr())); }
py::object mul(py::handle a, py::handle b) { return checked(PyNumber_Multiply(a.ptr(), b.ptr())); }
py::object div(py::handle a, py::handle b) { return checked(PyNumber_TrueDivide(a.ptr(), b.ptr())); }
bool truth(py::handle value) {
    const int result=PyObject_IsTrue(value.ptr());
    if (result < 0) throw py::error_already_set();
    return result != 0;
}
std::int64_t ns(py::handle value) { return py::cast<std::int64_t>(value); }
// Intern field names once per frame, not once per bar per window. Bar is the
// immutable shared protocol object; retaining handles keeps ownership explicit.
struct BarRow {
    py::object object;
    std::int64_t start,end;
    bool observed;
};

py::object frame(const std::string& input, const std::string& market,
                 std::int64_t max_age, std::int64_t cutoff, py::object observation,
                 py::iterable input_bars, py::object valuation) {
    const auto contract=py::module_::import("data.observation");
    const auto decimal=py::module_::import("decimal").attr("Decimal");
    const auto math=py::module_::import("math");
    const auto builtins=py::module_::import("builtins");
    const auto zero=decimal(0);
    const auto billion=py::int_(1000000000);
    py::dict values;
    for (auto name: contract.attr("EXECUTION_FEATURE_NAMES")) values[name]=py::none();
    py::list ages;
    py::object dependency=py::none();
    auto ready=[&](py::handle value) {
        if (dependency.is_none() || ns(value)>ns(dependency))
            dependency=py::reinterpret_borrow<py::object>(value);
    };
    if (!observation.is_none()) {
        auto book=observation.attr("payload");
        const auto age=cutoff-ns(observation.attr("source_asof_ns"));
        values["book_age_s"]=div(decimal(py::int_(age)),billion);
        ready(observation.attr("ready_ns"));
        ages.append(py::make_tuple(market,age));
        if (truth(book.attr("valid")) && age<=max_age) {
            auto bbo=book.attr("bbo").cast<py::tuple>();
            auto bidrow=bbo[0].cast<py::tuple>(), askrow=bbo[1].cast<py::tuple>();
            auto bid=bidrow[0], bq=bidrow[1], ask=askrow[0], aq=askrow[1];
            auto mid=div(add(bid,ask),py::int_(2));
            auto spread=sub(ask,bid);
            values["mid"]=mid;values["spread"]=spread;
            values["spread_bps"]=mul(div(spread,mid),py::int_(10000));
            values["weighted_mid_proxy"]=div(add(mul(ask,bq),mul(bid,aq)),add(bq,aq));
            auto bids=book.attr("bids").cast<py::tuple>(), asks=book.attr("asks").cast<py::tuple>();
            for (int depth: {5,20}) {
                if (py::len(bids)<depth || py::len(asks)<depth) continue;
                py::object bv=zero,av=zero;
                for (int i=0;i<depth;++i) {
                    bv=add(bv,bids[i].cast<py::tuple>()[1]);
                    av=add(av,asks[i].cast<py::tuple>()[1]);
                }
                const auto suffix=std::to_string(depth);
                values[py::str("depth_bid_"+suffix)]=bv;
                values[py::str("depth_ask_"+suffix)]=av;
                values[py::str("depth_imbalance_"+suffix)]=div(sub(bv,av),add(bv,av));
            }
        }
    }
    const py::str start_key("start_ns"), end_key("end_ns"), ready_key("ready_ns"), coverage_key("coverage");
    const py::str open_key("open"), close_key("close"), volume_key("volume"), count_key("observed_event_count");
    const py::str observed("observed");
    std::vector<BarRow> bars;
    for (auto bar: input_bars) {
        bars.push_back({py::reinterpret_borrow<py::object>(bar),ns(bar.attr(start_key)),
            ns(bar.attr(end_key)),py::object(bar.attr(coverage_key)).equal(observed)});
        ready(bar.attr(ready_key));
    }
    const auto end=bars.empty()?cutoff:bars.back().end;
    std::vector<const BarRow*> ten,sixty;
    for (int seconds: {10,30,60}) {
        std::vector<const BarRow*> selected;
        selected.reserve(seconds);
        for (const auto& b: bars) {
            if (end-static_cast<std::int64_t>(seconds)*1000000000<b.end && b.end<=end) selected.push_back(&b);
        }
        bool complete=selected.size()==static_cast<std::size_t>(seconds);
        for (std::size_t i=0;complete && i<selected.size();++i) {
            complete=selected[i]->observed;
            if (i && selected[i-1]->end!=selected[i]->start) complete=false;
        }
        if (!complete) continue;
        auto first=selected.front()->object.attr(open_key),last=selected.back()->object.attr(close_key);
        if (!first.is_none() && !last.is_none())
            values[py::str("return_"+std::to_string(seconds)+"s")]=sub(div(last,first),py::int_(1));
        if (seconds==10) ten=selected;
        if (seconds==60) sixty=selected;
    }
    auto sumfield=[&](const std::vector<const BarRow*>& rows,const char* field)->py::object {
        py::object total=zero;
        const py::str key(field);
        for (const auto& b: rows) {
            auto value=b->object.attr(key);
            if (value.is_none()) return py::none();
            total=add(total,value);
        }
        return total;
    };
    auto eventcount=[&](const std::vector<const BarRow*>& rows) {
        py::object total=py::int_(0);
        for (const auto& b: rows) total=add(total,b->object.attr(count_key));
        return total;
    };
    if (!ten.empty()) {
        for (auto field: {"volume","turnover","buy_volume","sell_volume","individual_count","native_packet_count"})
            values[py::str(std::string(field)+"_10s")]=sumfield(ten,field);
        values["observed_trade_count_10s"]=decimal(eventcount(ten));
        auto volume=values["volume_10s"];
        if (truth(volume)) {
            values["vwap_10s"]=div(values["turnover_10s"],volume);
            values["clock_volume_imbalance_10s"]=div(sub(values["buy_volume_10s"],values["sell_volume_10s"]),volume);
        }
    }
    if (!sixty.empty()) {
        py::object volume=zero;
        for (const auto& b:sixty) volume=add(volume,b->object.attr(volume_key));
        values["volume_60s"]=volume;
        bool all_closes=true;
        std::vector<py::object> closes;
        for (const auto& b:sixty) {
            closes.push_back(b->object.attr(close_key));
            if (closes.back().is_none()) all_closes=false;
        }
        if (all_closes) {
            py::list squares;
            for (std::size_t i=1;i<sixty.size();++i) {
                auto ratio=div(closes[i],closes[i-1]);
                auto r=math.attr("log")(builtins.attr("float")(ratio));
                squares.append(mul(r,r));
            }
            // Preserve Python3.12 compensated float sum and libm conversion.
            auto variance=div(builtins.attr("sum")(squares),py::int_(squares.size()));
            values["realized_vol_60s"]=decimal(py::str(math.attr("sqrt")(variance)));
        }
        auto slow=eventcount(sixty);
        if (!ten.empty() && truth(slow))
            values["trade_intensity_burst_guard"]=div(mul(values["observed_trade_count_10s"],py::int_(6)),slow);
    }
    if (!valuation.is_none()) {
        auto v=valuation.cast<py::tuple>();values["close"]=v[0];
        values["last_trade_age_s"]=div(decimal(py::int_(cutoff-ns(v[1]))),billion);
    }
    auto angle=mul(mul(div(py::int_(cutoff%86400000000000LL),py::int_(86400000000000LL)),py::int_(2)),math.attr("pi"));
    values["utc_hour_sin"]=decimal(py::str(math.attr("sin")(angle)));
    values["utc_hour_cos"]=decimal(py::str(math.attr("cos")(angle)));
    py::list validity;
    for (auto item:values) validity.append(py::make_tuple(item.first,!item.second.is_none()));
    return contract.attr("FeatureFrame")(input,contract.attr("CONTRACT"),contract.attr("FEATURE_CONTRACT"),
        cutoff,dependency,py::tuple(values.attr("items")()),py::tuple(ages),py::tuple(validity));
}
}

void bind_execution_features(py::module_& m) {
    m.attr("EXECUTION_FEATURE_FRAME_ABI")=1;
    m.def("execution_feature_frame_v1",&frame,py::arg("input_contract_id"),py::arg("market_id"),
          py::arg("max_book_age_ns"),py::arg("cutoff_ns"),py::arg("observation"),
          py::arg("bars"),py::arg("last_valuation"));
}
}
