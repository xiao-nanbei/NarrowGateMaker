#include "lightgbm_bundle.hpp"

#include <climits>
#include <cstdint>
#include <dlfcn.h>
#include <stdexcept>
#include <utility>

namespace narrowgate_cpp {
namespace {

using BoosterHandle = void*;
using FastConfigHandle = void*;
using CreateFromModelFile = int (*)(const char*, int*, BoosterHandle*);
using GetNumFeature = int (*)(BoosterHandle, int*);
using GetNumClasses = int (*)(BoosterHandle, int*);
using FastInit = int (*)(
    BoosterHandle,
    int,
    int,
    int,
    int,
    std::int32_t,
    const char*,
    FastConfigHandle*
);
using FastPredict = int (*)(FastConfigHandle, const void*, std::int64_t*, double*);
using FastFree = int (*)(FastConfigHandle);
using BoosterFree = int (*)(BoosterHandle);
using GetLastError = const char* (*)();

inline constexpr int kFloat64 = 1;
inline constexpr int kPredictNormal = 0;
inline constexpr char kSingleThreadParameters[] = "num_threads=1";

template <typename Function>
Function load_symbol(void* library, const char* name) {
    dlerror();
    void* raw = dlsym(library, name);
    const char* error = dlerror();
    if (error != nullptr || raw == nullptr) {
        std::string message = "missing LightGBM C API symbol ";
        message += name;
        if (error != nullptr) {
            message += ": ";
            message += error;
        }
        throw std::runtime_error(message);
    }
    return reinterpret_cast<Function>(raw);
}

}  // namespace

struct LightgbmBundleInference::State {
    void* library = nullptr;
    CreateFromModelFile create_from_model_file = nullptr;
    GetNumFeature get_num_feature = nullptr;
    GetNumClasses get_num_classes = nullptr;
    FastInit fast_init = nullptr;
    FastPredict fast_predict = nullptr;
    FastFree fast_free = nullptr;
    BoosterFree booster_free = nullptr;
    GetLastError get_last_error = nullptr;
    std::vector<BoosterHandle> boosters;
    std::vector<FastConfigHandle> fast_configs;

    ~State() {
        if (fast_free != nullptr) {
            for (FastConfigHandle handle : fast_configs) {
                if (handle != nullptr) {
                    static_cast<void>(fast_free(handle));
                }
            }
        }
        if (booster_free != nullptr) {
            for (BoosterHandle handle : boosters) {
                if (handle != nullptr) {
                    static_cast<void>(booster_free(handle));
                }
            }
        }
        if (library != nullptr) {
            static_cast<void>(dlclose(library));
        }
    }

    void check(int result, const char* operation) const {
        if (result == 0) {
            return;
        }
        std::string message = operation;
        if (get_last_error != nullptr) {
            const char* detail = get_last_error();
            if (detail != nullptr && detail[0] != '\0') {
                message += ": ";
                message += detail;
            }
        }
        throw std::runtime_error(message);
    }
};

LightgbmBundleInference::LightgbmBundleInference(
    std::string library_path,
    const std::vector<std::string>& model_paths,
    std::size_t feature_count
)
    : state_(std::make_unique<State>()),
      feature_count_(feature_count),
      library_path_(std::move(library_path)) {
    if (library_path_.empty()) {
        throw std::invalid_argument("LightGBM library path must not be empty");
    }
    if (model_paths.size() != kLightgbmBundleHeadNames.size()) {
        throw std::invalid_argument("LightGBM bundle requires exactly 13 model paths");
    }
    if (feature_count_ == 0 || feature_count_ > static_cast<std::size_t>(INT32_MAX)) {
        throw std::invalid_argument("LightGBM bundle feature count is invalid");
    }

    state_->library = dlopen(library_path_.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (state_->library == nullptr) {
        const char* detail = dlerror();
        throw std::runtime_error(
            std::string("failed to load active LightGBM library: ") +
            (detail == nullptr ? "unknown dlopen error" : detail)
        );
    }
    state_->create_from_model_file = load_symbol<CreateFromModelFile>(
        state_->library,
        "LGBM_BoosterCreateFromModelfile"
    );
    state_->get_num_feature = load_symbol<GetNumFeature>(
        state_->library,
        "LGBM_BoosterGetNumFeature"
    );
    state_->get_num_classes = load_symbol<GetNumClasses>(
        state_->library,
        "LGBM_BoosterGetNumClasses"
    );
    state_->fast_init = load_symbol<FastInit>(
        state_->library,
        "LGBM_BoosterPredictForMatSingleRowFastInit"
    );
    state_->fast_predict = load_symbol<FastPredict>(
        state_->library,
        "LGBM_BoosterPredictForMatSingleRowFast"
    );
    state_->fast_free = load_symbol<FastFree>(
        state_->library,
        "LGBM_FastConfigFree"
    );
    state_->booster_free = load_symbol<BoosterFree>(
        state_->library,
        "LGBM_BoosterFree"
    );
    state_->get_last_error = load_symbol<GetLastError>(
        state_->library,
        "LGBM_GetLastError"
    );

    state_->boosters.reserve(model_paths.size());
    state_->fast_configs.reserve(model_paths.size());
    for (const std::string& model_path : model_paths) {
        BoosterHandle booster = nullptr;
        int iterations = 0;
        state_->check(
            state_->create_from_model_file(model_path.c_str(), &iterations, &booster),
            "failed to load LightGBM model"
        );
        static_cast<void>(iterations);
        state_->boosters.push_back(booster);

        int model_feature_count = 0;
        state_->check(
            state_->get_num_feature(booster, &model_feature_count),
            "failed to read LightGBM model feature count"
        );
        if (model_feature_count != static_cast<int>(feature_count_)) {
            throw std::runtime_error("LightGBM model feature count mismatch");
        }

        int model_class_count = 0;
        state_->check(
            state_->get_num_classes(booster, &model_class_count),
            "failed to read LightGBM model class count"
        );
        if (model_class_count != 1) {
            throw std::runtime_error(
                "LightGBM bundle requires one scalar output per model"
            );
        }

        FastConfigHandle fast_config = nullptr;
        state_->check(
            state_->fast_init(
                booster,
                kPredictNormal,
                0,
                -1,
                kFloat64,
                static_cast<std::int32_t>(feature_count_),
                kSingleThreadParameters,
                &fast_config
            ),
            "failed to initialize LightGBM single-row predictor"
        );
        state_->fast_configs.push_back(fast_config);
    }
}

LightgbmBundleInference::~LightgbmBundleInference() = default;

void LightgbmBundleInference::predict(
    std::span<const double> row,
    std::span<double> output
) const {
    if (row.size() != feature_count_) {
        throw std::invalid_argument("LightGBM prediction row width mismatch");
    }
    if (output.size() != kLightgbmBundleHeadNames.size()) {
        throw std::invalid_argument("LightGBM prediction output width mismatch");
    }
    for (std::size_t index = 0; index < state_->fast_configs.size(); ++index) {
        std::int64_t output_length = 0;
        state_->check(
            state_->fast_predict(
                state_->fast_configs[index],
                row.data(),
                &output_length,
                output.data() + index
            ),
            "LightGBM single-row prediction failed"
        );
        if (output_length != 1) {
            throw std::runtime_error(
                "LightGBM single-row prediction returned an invalid output width"
            );
        }
    }
}

std::size_t LightgbmBundleInference::feature_count() const noexcept {
    return feature_count_;
}

const std::string& LightgbmBundleInference::library_path() const noexcept {
    return library_path_;
}

}  // namespace narrowgate_cpp
