#include <ydb/library/yql/minikql/mkql_alloc.h>
#include "dq_metrics_accumulator.h"
#include "util/system/guard.h"
#include "util/system/mutex.h"
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <ydb/library/yql/public/udf/arrow/args_dechunker.h>
#include <ydb/library/yql/public/udf/arrow/memory_pool.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {
namespace NDq {

bool operator==(const StageId& lhs, const StageId& rhs) {
    return lhs.dest == rhs.dest && lhs.src == rhs.src;
}

MetricsAccumulator::MetricsAccumulator(const std::string& OutputFile)
    : Results(OutputFile)
{
    TGuard<TMutex> l{mutex};
    YQL_ENSURE(Results);
    for (int i = 0; i < 30; i++) {
        CurrentStage().loadHistogramm[i];
    }
}

void MetricsAccumulator::RememberLoad(std::size_t partition, ui64 bytes, ui64 rowsProcessed) {
    TGuard<TMutex> l{mutex};
    CurrentStage().loadHistogramm[partition].callTimes++;
    (void)bytes;
    CurrentStage().loadHistogramm[partition].rowsCount += rowsProcessed;
}

void MetricsAccumulator::AddType(NUdf::TDataTypeId type) {
    TGuard<TMutex> l{mutex};
    CurrentStage().typeInfo[type]++;
}

void MetricsAccumulator::AddShuffle() {CurrentStage().shuffleTimes++;}

MetricsAccumulator::~MetricsAccumulator() {
    TGuard<TMutex> l{mutex};

    for (const auto& [stage, stageMetrics] : sm) {
        Results << "\nStageId = " << stage
                << "\nUniqueTypes = " << stageMetrics.typeInfo.size()
                << "\nShuffleTimes = " << stageMetrics.shuffleTimes
                << "\nPartition, CallTimes, RowsProcessed\n";
        for (const auto& [partition, metrics] : stageMetrics.loadHistogramm) {
            Results << partition << ", " << metrics.callTimes
                    << ", " << metrics.rowsCount
                    << "\n";
        }
    }
}

void MetricsAccumulator::SetDstStageId(ui32 v) {
    TGuard<TMutex> l{mutex};
    newDstStageId = v;
}

void MetricsAccumulator::SetSrcStageId(ui32 v) {
    TGuard<TMutex> l{mutex};
    newSrcStageId = v;
}

void InitStage(MetricsAccumulator::StageMetrics& m) {
     for (int i = 0; i < 30; i++) {
        m.loadHistogramm[i];
     }
}

void MetricsAccumulator::MaybeNewStage() {
    TGuard<TMutex> l{mutex};
    bool changed = srcStageId != newSrcStageId ||
                   dstStageId != newDstStageId;

    srcStageId = newSrcStageId;
    dstStageId = newDstStageId;

    if (changed) {
        InitStage(CurrentStage());
    }
}

std::string DumpName() {
    std::string name = "info_dump/q" + GetEnv("QUERY_NUM") + ".csv";
    return name;
}

MetricsAccumulator& GetMetricsAccumulator() {
    static MetricsAccumulator results(DumpName());
    return results;
}

std::ostream& operator<<(std::ostream& out, StageId id) {
    out << "(" << id.dest << ", " << id.src << ")";
    return out;
}


} // namespace NDq
} // namespace NYql
