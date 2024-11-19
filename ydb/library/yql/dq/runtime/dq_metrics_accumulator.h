#pragma once

#include "boost/container_hash/hash_fwd.hpp"
#include "library/cpp/yt/misc/hash.h"
#include "util/system/mutex.h"
#include <fstream>
#include <unordered_map>
#include <util/system/env.h>
#include <map>
#include <ydb/library/yql/public/udf/udf_data_type.h>

namespace NYql {
namespace NDq {

struct StageId {
    ui32 dest;
    ui32 src;
};

bool operator==(const StageId& lhs, const StageId& rhs);

struct StageHasher {
    std::size_t operator()(const StageId& s) const {
        std::size_t seed = 37 * 37 * s.dest + 37 * s.src;
        return seed;
    }
};

class MetricsAccumulator {
public:
    struct Metrics {
        ui64 callTimes = 0;
        ui64 bytesProcessed = 0;
        ui64 rowsCount = 0;
    };

    struct StageMetrics {
        std::map<std::size_t, Metrics> loadHistogramm;
        std::map<NUdf::TDataTypeId, ui64> typeInfo;
        ui64 shuffleTimes = 0;
    };

    using StageMap = std::unordered_map<StageId, StageMetrics, StageHasher>;
public:
    MetricsAccumulator(const std::string& OutputFile);

    void RememberLoad(std::size_t partition, ui64 bytes, ui64 rowsProcessed);

    void AddType(NUdf::TDataTypeId type);

    void AddShuffle();

    ~MetricsAccumulator();

    void SetDstStageId(ui32 v);

    void SetSrcStageId(ui32 v);

    void MaybeNewStage();
private:
    StageMetrics& CurrentStage() {
        return sm[StageId{dstStageId, srcStageId}];
    }
private:
    std::ofstream Results;
    StageMap sm;
    ui32 srcStageId;
    ui32 dstStageId;
    ui32 newSrcStageId;
    ui32 newDstStageId;
    TMutex mutex;
};

std::string DumpName();

MetricsAccumulator& GetMetricsAccumulator();

std::ostream& operator<<(std::ostream& out, StageId);

} // namespace NDq
} // namespace NYql
