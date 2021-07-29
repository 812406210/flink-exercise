package streamPredict;

import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GbdtRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.GbdtRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-24 09:45
 */
public class testGbdtRegPredictStreamOp {
    public static void main(String[] args) throws Exception {
        //1、数据处理
        List <Row> df = Arrays.asList(
                Row.of(1.0, "A", 0, 0, 0),
                Row.of(2.0, "B", 1, 1, 0),
                Row.of(3.0, "C", 2, 2, 1),
                Row.of(4.0, "D", 3, 3, 1)
        );
        BatchOperator <?> batchSource = new MemSourceBatchOp(
                df, new String[]{"f0", "f1", "f2", "f3", "label"});
        //2、训练
        BatchOperator <?> trainOp = new GbdtRegTrainBatchOp()
                .setLearningRate(1.0)
                .setNumTrees(3)
                .setMinSamplesPerLeaf(1)
                .setLabelCol("label")
                .setFeatureCols("f0", "f1", "f2", "f3")
                .linkFrom(batchSource);

        //3、预测
        BatchOperator <?> predictBatchOp = new GbdtRegPredictBatchOp()
                .setPredictionCol("pred");

        //4、pipline设置，进行数据预测
        BatchOperator<?> resultOp = predictBatchOp.linkFrom(trainOp, batchSource);
        resultOp.print();

        //5、结果集保存至Csv文件
        CsvSinkBatchOp csvSink = new CsvSinkBatchOp();
        csvSink.setFilePath("E:\\Flink\\FlinkSql\\FlinkML\\model\\testGbdtRegPredictStreamOp");
        predictBatchOp.link(csvSink);

        BatchOperator.execute();
    }
}
