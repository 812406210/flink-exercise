package RegressionPredict;


import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
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
 * @create: 2021-06-23 16:24
 */
public class GbdtRegPredictBatchOpTest {
    public static void main(String[] args) throws Exception {
        List<Row> df = Arrays.asList(
                Row.of(1.0, "A", 0, 0, 0),
                Row.of(2.0, "B", 1, 1, 0),
                Row.of(3.0, "C", 2, 2, 1),
                Row.of(4.0, "D", 3, 3, 1)
        );
        //方式一:如果不指定数据类型，则程序根据值去猜数据类型，
//        BatchOperator <?> batchSource = new MemSourceBatchOp(df, new String[] {"f0", "f1", "f2", "f3", "label"});
//        StreamOperator <?> streamSource = new MemSourceStreamOp(df, new String[] {"f0", "f1", "f2", "f3", "label"});

        //方式二
        BatchOperator <?> batchSource = new MemSourceBatchOp(df, new TableSchema("f0,f1,f2,f3,label".split(","),
                new TypeInformation[] {Types.DOUBLE, Types.STRING,Types.INT,Types.INT,Types.INT}));
        StreamOperator <?> streamSource = new MemSourceStreamOp(df,new TableSchema("f0,f1,f2,f3,label".split(","),
                new TypeInformation[] {Types.DOUBLE, Types.STRING,Types.INT,Types.INT,Types.INT}));

        BatchOperator <?> trainOp = new GbdtRegTrainBatchOp()
                .setLearningRate(1.0)
                .setNumTrees(3)
                .setMinSamplesPerLeaf(1)
                .setLabelCol("label")
                .setFeatureCols("f0", "f1", "f2", "f3")
                .linkFrom(batchSource);
        BatchOperator <?> predictBatchOp = new GbdtRegPredictBatchOp()
                .setPredictionCol("pred");
        StreamOperator <?> predictStreamOp = new GbdtRegPredictStreamOp(trainOp)
                .setPredictionCol("pred");
        System.out.println(">>>>>>>>>批量>>>>>>>>>>");
        predictBatchOp.linkFrom(trainOp, batchSource).print();
        System.out.println(">>>>>>>>>流式>>>>>>>>>>");
        predictStreamOp.linkFrom(streamSource).print();
        StreamOperator.execute();
    }
}
