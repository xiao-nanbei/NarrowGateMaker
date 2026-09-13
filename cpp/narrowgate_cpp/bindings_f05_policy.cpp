#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "binding_registry.hpp"
#include "f05_cooldown_types.hpp"

namespace py = pybind11;

namespace narrowgate_cpp {

void bind_f05_policy_types(py::module_& m) {
    py::enum_<F05PredicateMetric>(m, "F05PredicateMetric")
        .value("CAMPAIGN_AGE_GT_CONTROL", F05PredicateMetric::CampaignAgeGtControl)
        .value("POSITIVE_ORDERING", F05PredicateMetric::PositiveOrdering)
        .value("LAST_CROSS_POSITIVE", F05PredicateMetric::LastCrossPositive)
        .value("EXPANDING", F05PredicateMetric::Expanding)
        .value("CONVERGING", F05PredicateMetric::Converging)
        .value("ABS_DISTANCE", F05PredicateMetric::AbsDistance)
        .value("CROSS_AGE_S", F05PredicateMetric::CrossAgeS)
        .value(
            "ARRANGEMENT_PERSISTENCE_S",
            F05PredicateMetric::ArrangementPersistenceS)
        .value("SIGNED_DISTANCE", F05PredicateMetric::SignedDistance)
        .value(
            "SIGNED_DISTANCE_VELOCITY",
            F05PredicateMetric::SignedDistanceVelocity)
        .value(
            "SIGNED_DISTANCE_ACCELERATION",
            F05PredicateMetric::SignedDistanceAcceleration);

    py::class_<F05BooleanLiteral>(m, "F05BooleanLiteral")
        .def(py::init<>())
        .def_readwrite("predicate_index", &F05BooleanLiteral::predicate_index)
        .def_readwrite("negated", &F05BooleanLiteral::negated);
    py::class_<F05BooleanClause>(m, "F05BooleanClause")
        .def(py::init<>())
        .def_readwrite("literals", &F05BooleanClause::literals);
    py::class_<F05BooleanRule>(m, "F05BooleanRule")
        .def(py::init<>())
        .def_readwrite("action_id", &F05BooleanRule::action_id)
        .def_readwrite("duration_ms", &F05BooleanRule::duration_ms)
        .def_readwrite("clauses", &F05BooleanRule::clauses);
    py::class_<F05PredicatePair>(m, "F05PredicatePair")
        .def(py::init<>())
        .def_readwrite("fast_ema_index", &F05PredicatePair::fast_ema_index)
        .def_readwrite("slow_ema_index", &F05PredicatePair::slow_ema_index);
    py::class_<F05PredicateDefinition>(m, "F05PredicateDefinition")
        .def(py::init<>())
        .def_readwrite(
            "predicate_index", &F05PredicateDefinition::predicate_index)
        .def_readwrite("metric", &F05PredicateDefinition::metric)
        .def_readwrite("pair_index", &F05PredicateDefinition::pair_index)
        .def_readwrite(
            "threshold_enabled", &F05PredicateDefinition::threshold_enabled)
        .def_readwrite("threshold", &F05PredicateDefinition::threshold);
    py::class_<F05BooleanPolicy>(m, "F05BooleanPolicy")
        .def(py::init<>())
        .def_readwrite("policy_sha256", &F05BooleanPolicy::policy_sha256)
        .def_readwrite(
            "predicate_bundle_sha256",
            &F05BooleanPolicy::predicate_bundle_sha256)
        .def_readwrite("predicate_columns", &F05BooleanPolicy::predicate_columns)
        .def_readwrite("rules", &F05BooleanPolicy::rules)
        .def_readwrite("ema_half_lives_s", &F05BooleanPolicy::ema_half_lives_s)
        .def_readwrite("predicate_pairs", &F05BooleanPolicy::predicate_pairs)
        .def_readwrite(
            "predicate_definitions", &F05BooleanPolicy::predicate_definitions)
        .def_readwrite("default_action", &F05BooleanPolicy::default_action);
}

}  // namespace narrowgate_cpp
