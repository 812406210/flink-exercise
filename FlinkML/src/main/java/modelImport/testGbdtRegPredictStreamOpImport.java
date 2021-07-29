package modelImport;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GbdtRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.classification.RandomForestClassifier;
import com.alibaba.alink.pipeline.tuning.*;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-24 09:45
 */
public class testGbdtRegPredictStreamOpImport {
    public static void main(String[] args) throws Exception {
        //1、读取数据模型
        String filePath = "E:\\Flink\\FlinkSql\\FlinkML\\model\\testGbdtRegPredictStreamOpModel";

        AkSourceBatchOp trainOp = new AkSourceBatchOp().setFilePath(filePath);

        //2、测试数据
        List <Row> df = Arrays.asList(
                Row.of(6.0, "A", 3, 3),
                Row.of(10.0, "A", 1, 1),
                Row.of(8.0, "C", 2, 2),
                Row.of(9.0, "D", 3, 3)
        );
        BatchOperator <?> batchSource = new MemSourceBatchOp(
                df, new String[]{"f0", "f1", "f2", "f3"});

        //3、预测初始化
        BatchOperator <?> predictBatchOp = new GbdtRegPredictBatchOp()
                .setPredictionCol("pred");

        //4、数据预测
        BatchOperator<?> resultOp = predictBatchOp.linkFrom(trainOp, batchSource);

        System.out.println(">>>>>>>>>>>>>>预测结果数据>>>>>>>>>>>>");
        resultOp.print();

    }
}
