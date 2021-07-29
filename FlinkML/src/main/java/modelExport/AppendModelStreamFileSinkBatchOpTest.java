package modelExport;

import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;

import java.util.Arrays;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-23 17:39
 */
public class AppendModelStreamFileSinkBatchOpTest {
    public static void main(String[] args) throws Exception {
        Row[] rows = new Row[] {
                Row.of(1.0, "A", 0L, 0, 0, 1.0),
                Row.of(2.0, "B", 1L, 1, 0, 2.0),
                Row.of(3.0, "C", 2L, 2, 1, 3.0),
                Row.of(4.0, "D", 3L, 3, 1, 4.0)
        };
        String[] colNames = new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"};
        String labelColName = colNames[4];
        MemSourceBatchOp input = new MemSourceBatchOp(
                Arrays.asList(rows), new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"}
        );
        RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
                .setLabelCol(labelColName)
                .setFeatureCols(colNames[0], colNames[1], colNames[2], colNames[3])
                .setFeatureSubsamplingRatio(0.5)
                .setSubsamplingRatio(1.0)
                .setNumTreesOfInfoGain(1)
                .setNumTreesOfInfoGain(1)
                .setNumTreesOfInfoGainRatio(1)
                .setCategoricalCols(colNames[1]);
        rfOp.linkFrom(input).link(
                new AppendModelStreamFileSinkBatchOp()
                        .setFilePath("E:\\Flink\\FlinkSql\\FlinkML\\model\\random_forest_model_stream")
                        .setNumKeepModel(10)
        );
        BatchOperator.execute();
    }
}
