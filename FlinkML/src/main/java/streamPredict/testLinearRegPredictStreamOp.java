package streamPredict;
import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.LinearRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-23 17:47
 */
public class testLinearRegPredictStreamOp {
    public static void main(String[] args) throws Exception {
        List <Row> df = Arrays.asList(
                Row.of(2, 1, 1),
                Row.of(3, 2, 1),
                Row.of(4, 3, 2),
                Row.of(2, 4, 1),
                Row.of(2, 2, 1),
                Row.of(4, 3, 2),
                Row.of(1, 2, 1)
        );
        BatchOperator <?> batchData = new MemSourceBatchOp(df, new String[]{"f0", "f1","label"});
        StreamOperator <?> streamData = new MemSourceStreamOp(df, new String[]{"f0", "f1","label"});
        String[] colnames = new String[] {"f0", "f1"};
        BatchOperator <?> lr = new LinearRegTrainBatchOp()
                .setFeatureCols(colnames)
                .setLabelCol("label");

        BatchOperator <?> model = batchData.link(lr);
        StreamOperator <?> predictor = new LinearRegPredictStreamOp(model)
                .setPredictionCol("pred");
        predictor.linkFrom(streamData).print();
        StreamOperator.execute();
    }
}
