package modelImport;

import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.RandomForestPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-23 18:11
 */
public class RandomForestPredictStreamOpTest {
    public static void main(String[] args) throws Exception {
        List <Row> df = Arrays.asList(
                Row.of(1.0, "A", 0, 0, 0),
                Row.of(2.0, "B", 1, 1, 0),
                Row.of(3.0, "C", 2, 2, 1),
                Row.of(4.0, "D", 3, 3, 1)
        );
        BatchOperator <?> batchSource = new MemSourceBatchOp(df, new String[]{"f0", "f1", "f2" , "f3" , "label" });

        StreamOperator <?> streamSource = new MemSourceStreamOp(df,  new String[]{"f0", "f1", "f2" , "f3" , "label" });

        BatchOperator <?> trainOp = new RandomForestTrainBatchOp()
                .setLabelCol("label")
                .setFeatureCols("f0", "f1", "f2", "f3")
                .linkFrom(batchSource);

        StreamOperator <?> predictStreamOp = new RandomForestPredictStreamOp(trainOp)
                .setModelStreamFilePath("E:\\Flink\\FlinkSql\\FlinkML\\model\\random_forest_model_stream")
                .setPredictionDetailCol("pred_detail")
                .setPredictionCol("pred");

        predictStreamOp.linkFrom(streamSource).print();
        StreamOperator.execute();
    }
}
