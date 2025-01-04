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
    YQL_ENSURE(Results.good());
}

void MetricsAccumulator::RememberLoad(const StageId& stage, std::size_t partition, ui64 bytes, ui64 rowsProcessed) {
    TGuard<TMutex> l{mutex};
    sm[stage].loadHistogramm[partition].callTimes++;
    (void)bytes;
    sm[stage].loadHistogramm[partition].rowsCount += rowsProcessed;
}

void MetricsAccumulator::AddType(const StageId& stage, NUdf::TDataTypeId type) {
    TGuard<TMutex> l{mutex};
    sm[stage].typeInfo[type]++;
}

MetricsAccumulator::~MetricsAccumulator() {
    TGuard<TMutex> l{mutex};
    for (const auto& [stage, stageMetrics] : sm) {
        Results << "\nTransition: " << stage
                << "\nPartition, RowsProcessed\n";
        for (const auto& [partition, metrics] : stageMetrics.loadHistogramm) {
            Results << partition << ", " << metrics.rowsCount << "\n";
        }
    }
    Results << "\n ========================================== \n";
    std::unordered_map<std::size_t, std::map<ui64, ui64>> results;
    for (const auto& [stage, stageMetrics] : sm) {

        for (const auto& [p, metrics] : stageMetrics.loadHistogramm) {
            results[stage.dest][p] += metrics.rowsCount;
        }
    }


    for (const auto& [stage, hist]: results) {
        Results << "\nStage: " << stage << "\n";
        for (const auto& [partition, load] : hist) {
            Results << "\t" << partition << ": " << load << "\n";
        }
    }

}

void InitStage(MetricsAccumulator::StageMetrics& m) {
     for (int i = 0; i < 30; i++) {
        m.loadHistogramm[i];
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
    out << "(" << id.src << " -> " << id.dest << ")";
    return out;
}


} // namespace NDq
} // namespace NYql
