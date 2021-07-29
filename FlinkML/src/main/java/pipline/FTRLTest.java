package pipline;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-23 18:58
 */
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.dataproc.format.JsonToCsvStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp;
import com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp;
import com.alibaba.alink.operator.stream.sink.TextSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.FeatureHasher;

public class FTRLTest {
    public static void main(String[] args) throws Exception {

//        new TextSourceBatchOp()
//                .setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-small.csv");
        //根据数据设置列
        String schemaStr
                = "id string, click string, dt string, C1 string, banner_pos int, site_id string, site_domain string, "
                + "site_category string, app_id string, app_domain string, app_category string, device_id string, "
                + "device_ip string, device_model string, device_type string, device_conn_type string, C14 int, C15 int, "
                + "C16 int, C17 int, C18 int, C19 int, C20 int, C21 int";
        CsvSourceBatchOp trainBatchData = new CsvSourceBatchOp()
                .setFilePath("E:\\Flink\\FlinkSql\\FlinkML\\data\\avazu-small.csv")
                .setSchemaStr(schemaStr);


        //设置标签列
        String labelColName = "click";
        //设置选择的属性
        String[] selectedColNames = new String[] {
                "C1", "banner_pos", "site_category", "app_domain",
                "app_category", "device_type", "device_conn_type",
                "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21",
                "site_id", "site_domain", "device_id", "device_model"};
        //（枚举）类别型特征
        String[] categoryColNames = new String[] {
                "C1", "banner_pos", "site_category", "app_domain",
                "app_category", "device_type", "device_conn_type",
                "site_id", "site_domain", "device_id", "device_model"};
        //数值型特征
        String[] numericalColNames = new String[] {
                "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21"};

        // 标准化后的结果向量名称
        String vecColName = "vec";
        //向量长度
        int numHashFeatures = 30000;

        //设置特征工程工作流管道
        Pipeline feature_pipeline = new Pipeline()
                .add(
                        //数值特征标准化
                        new StandardScaler()
                                .setSelectedCols(numericalColNames)
                )
                .add(
                        //将多个特征组合成一个特征向量
                        new FeatureHasher()
                                .setSelectedCols(selectedColNames)
                                //离散特征列
                                .setCategoricalCols(categoryColNames)
                                //设置输出列名
                                .setOutputCol(vecColName)
                                //设置生成向量长度
                                .setNumFeatures(numHashFeatures)
                );
        // 训练并保存数据模型
        String FEATURE_PIPELINE_MODEL_FILE =  "E:\\Flink\\FlinkSql\\FlinkML\\model\\feature_pipe_model.csv";
        feature_pipeline.fit(trainBatchData).save(FEATURE_PIPELINE_MODEL_FILE,true);

        BatchOperator.execute();

        // 原始数据集
        CsvSourceStreamOp data = new CsvSourceStreamOp()
                .setFilePath("E:\\Flink\\FlinkSql\\FlinkML\\model\\avazu-ctr-train-8M.csv")
                .setSchemaStr(schemaStr);

        // 将原始数据集按1：1分为流式训练集，流式测试集
        SplitStreamOp spliter = new SplitStreamOp().setFraction(0.5).linkFrom(data);
        StreamOperator train_stream_data = spliter;
        StreamOperator test_stream_data = spliter.getSideOutput(0);

        // 载入之前的特征工程模型
        PipelineModel feature_pipelineModel = PipelineModel.load(FEATURE_PIPELINE_MODEL_FILE);

        /**
         * 分别利用模型生成批式训练数据，流式训练数据，流式测试数据
         * **/

        //将处理好的批式训练数据调入逻辑回归算子，计算出初始模型
        //初始模型设置
        LogisticRegressionTrainBatchOp lr = new LogisticRegressionTrainBatchOp()
                //向量列名
                .setVectorCol(vecColName)
                //标签列名
                .setLabelCol(labelColName)
                //是否有常数项
                .setWithIntercept(true)
                //最大迭代步数
                .setMaxIter(10);

        //连接批处理数据
        BatchOperator initModel = feature_pipelineModel.transform(trainBatchData).link(lr);

        /**
         * FTRL模型训练及预测评估
         * **/

        // FTRL流式训练
        FtrlTrainStreamOp model = new FtrlTrainStreamOp(initModel)
                //特征向量名
                .setVectorCol(vecColName)
                //标签列名
                .setLabelCol(labelColName)
                //有常数项
                .setWithIntercept(true)
                //参数α的值
                .setAlpha(0.1)
                //参数β的值
                .setBeta(0.1)
                //L1 正则化系数
                .setL1(0.01)
                //L2 正则化系数
                .setL2(0.01)
                //数据流流动过程中时间的间隔（窗口大小）
                .setTimeInterval(10)
                //向量长度
                .setVectorSize(numHashFeatures)
                //模型连接流式训练数据
                .linkFrom(feature_pipelineModel.transform(train_stream_data));

        // FTRL流式预测
        FtrlPredictStreamOp predResult = new FtrlPredictStreamOp(initModel)
                //向量列名
                .setVectorCol(vecColName)
                //预测结果列名
                .setPredictionCol("pred")
                //算法保留列名
                .setReservedCols(new String[] {labelColName})
                //预测详细信息列名
                .setPredictionDetailCol("details")
                //模型连接模型流和流式测试数据
                .linkFrom(model, feature_pipelineModel.transform(test_stream_data));

        //取样输出
        predResult.sample(0.01).print();

        StreamOperator.execute();

        /**
         * 模型流式二分类评估
         * **/
        predResult
                .link(
                        new EvalBinaryClassStreamOp()
                                .setLabelCol(labelColName)
                                .setPredictionCol("pred")
                                .setPredictionDetailCol("details")
                                .setTimeInterval(10)
                )
                .link(
                        new JsonValueStreamOp()
                                .setSelectedCol("Data")
                                .setReservedCols(new String[] {"Statistics"})
                                .setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
                                .setJsonPath(new String[] {"$.Accuracy", "$.AUC", "$.ConfusionMatrix"})
                )
                .print();
        StreamOperator.execute();

//        EvalBinaryClassStreamOp eval = new EvalBinaryClassStreamOp()
//                //标签名
//                .setLabelCol(labelColName)
//                //设置预测结果名
//                .setPredictionCol("pred")
//                //预测详细信息列名
//                .setPredictionDetailCol("details")
//                //流式数据统计的时间间隔（窗口值为10s）
//                .setTimeInterval(1.0)
//                .linkFrom(predResult);
        //评估结果为json格式，故连接json解析器并打印之，选取精度，AUC，混淆矩阵三个指标
//        String[] colNames = eval.getColNames();
//        JsonToCsvStreamOp toCsvData = new JsonToCsvStreamOp()
//                .setCsvCol("CsvData")
//                .setJsonCol(colNames[0])
//                .setSchemaStr("Accuracy string, AUC string, ConfusionMatrix string");
//        CsvSinkStreamOp Out = new CsvSinkStreamOp()
//                .setFilePath("D:/Study_data/FlinkStudy/OutPutData/eval_out.csv");
//
//        eval.link(toCsvData).link(Out)；


    }
}