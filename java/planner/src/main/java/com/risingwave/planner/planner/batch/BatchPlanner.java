package com.risingwave.planner.planner.batch;

import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CBO;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_REWRITE;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.PHYSICAL;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_REWRITE_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_AGG_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_JOIN_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.JoinReorderProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.SubQueryRewriteProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rules.BatchRuleSets;
import com.risingwave.planner.sql.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Planner for batch query */
public class BatchPlanner implements Planner<BatchPlan> {
  private static final Logger log = LoggerFactory.getLogger(BatchPlanner.class);

  public BatchPlanner() {}

  @Override
  public BatchPlan plan(SqlNode ast, ExecutionContext context) {
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelNode rawPlan = sqlConverter.toRel(ast).rel;

    OptimizerProgram optimizerProgram = buildOptimizerProgram(!isSingleMode(context));

    RelNode result = optimizerProgram.optimize(rawPlan, context);
    RisingWaveBatchPhyRel root = (RisingWaveBatchPhyRel) result;
    log.info("Create physical plan:\n {}", ExplainWriter.explainPlan(root));

    return new BatchPlan(root);
  }

  private static OptimizerProgram buildOptimizerProgram(boolean isDistributed) {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder();

    builder.addLast(SUBQUERY_REWRITE, SubQueryRewriteProgram.INSTANCE);

    builder.addLast(
        LOGICAL_REWRITE, HepOptimizerProgram.builder().addRules(LOGICAL_REWRITE_RULES).build());

    builder.addLast(JOIN_REORDER, JoinReorderProgram.INSTANCE);

    builder.addLast(
        LOGICAL_CBO,
        VolcanoOptimizerProgram.builder()
            .addRules(BatchRuleSets.LOGICAL_OPTIMIZE_RULES)
            .addRules(LOGICAL_CONVERTER_RULES)
            .addRules(LOGICAL_OPTIMIZATION_RULES)
            .addRequiredOutputTraits(LOGICAL)
            .build());

    var physical =
        VolcanoOptimizerProgram.builder()
            .addRules(PHYSICAL_CONVERTER_RULES)
            .addRules(PHYSICAL_AGG_RULES)
            .addRules(PHYSICAL_JOIN_RULES)
            .addRequiredOutputTraits(RisingWaveBatchPhyRel.BATCH_PHYSICAL);

    if (isDistributed) {
      physical.addRequiredOutputTraits(RwDistributions.SINGLETON);
    }

    builder.addLast(PHYSICAL, physical.build());

    return builder.build();
  }
}
