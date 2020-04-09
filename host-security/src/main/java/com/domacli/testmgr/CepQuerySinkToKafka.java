package com.domacli.testmgr;

import com.alibaba.fastjson.JSON;
import com.domacli.testmgr.udf.TrojanQuery;
import com.domacli.testmgr.util.IpUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.io.*;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Properties;


public class CepQuerySinkToKafka {

    private static String getLocalIP() {
        try {
            return IpUtil.getLocalHostLANAddress().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    public static void main(String[] args) throws Exception {

        String defaultPropertiesFile = "/data/svr/projects/flink/sql.properties";
        String zk = null;
        String kafka = null;
        String inputSQL = null;
        String sqlKey = null;

        String localIp = getLocalIP();

        InputStream in = null;
        FileOutputStream oFile = null;

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            zk = parameterTool.get("zk");
            kafka = parameterTool.get("kafka");
            inputSQL = parameterTool.get("inputSQL");
            sqlKey = parameterTool.get("key");

            if (inputSQL == null || inputSQL.equals("")) {
                if (sqlKey == null) {
                    sqlKey = "default";
                }
                //ParameterTool pt = ParameterTool.fromPropertiesFile(defaultPropertiesFile);
                //inputSQL = pt.get(sqlKey);

                in = new BufferedInputStream(new FileInputStream(defaultPropertiesFile));
                Properties prop = new Properties();
                prop.load(new InputStreamReader(in, "utf-8"));
                inputSQL = prop.getProperty(sqlKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        if (null == zk) {
            zk = localIp + ":2181";
        }

        if (null == kafka) {
            kafka = localIp + ":9092";
        }

        if (null == sqlKey) {
            sqlKey = "default";
        }
        if (null == inputSQL) {
            inputSQL = "SELECT * FROM agentdata";
        }

        int parallelNum = 1;

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(parallelNum);

        //定义 Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        Properties properties = new Properties();
        properties.put("zookeeper.connect", zk);
        properties.put("bootstrap.servers", kafka);
        properties.put("group.id", sqlKey);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "5000");

        tableEnv.connect(new Kafka().version("0.11").properties(properties).topic("edrlog").startFromGroupOffsets())
                .withFormat(new Json().deriveSchema())
                //数据表的schema
                .withSchema(new Schema()
                        .field("Ver", Types.STRING)
                        .field("AgentVer", Types.STRING)
                        .field("Mid", Types.STRING)
                        .field("Plugin", Types.STRING)
                        .field("Tag", Types.STRING)
                        .field("Time", Types.LONG)
                        .field("process_timestamp", Types.SQL_TIMESTAMP)
                        .field("Type", Types.INT)
                        .field("Op", Types.INT)
                        .field("OptPid", Types.LONG)
                        .field("OptProcPath", Types.STRING)
                        .field("OptFileSize", Types.INT)
                        .field("OptModifyTime", Types.LONG)
                        .field("OptProcName", Types.STRING)
                        .field("OptCmdline", Types.STRING)
                        .field("OptPPid", Types.LONG)
                        .field("OptStdin", Types.STRING)
                        .field("OptStdout", Types.STRING)
                        .field("OptProcMd5", Types.STRING)
                        .field("OptProcUserName", Types.STRING)
                        .field("OptProcUid", Types.LONG)
                        .field("OptProcGroupName", Types.STRING)
                        .field("OptProcGid", Types.LONG)
                        .field("OptScanResult", Types.INT)
                        .field("OptScanExt", Types.INT)
                        .field("OptScanVirusName", Types.STRING)
                        .field("OptProcPName", Types.STRING)
                        .field("OptProcPPath", Types.STRING)
                        .field("OptProcPCmdline", Types.STRING)
                        .field("OptProcPUsername", Types.STRING)
                        .field("OptProcPUid", Types.LONG)
                        .field("OptProcPGroupName", Types.STRING)
                        .field("OptProcPGid", Types.LONG)
                        .field("OptProcPidTree", Types.STRING)
                        .field("OptProcPStdin", Types.STRING)
                        .field("OptProcPStdout", Types.STRING)
                        .field("OptFilePMd5", Types.STRING)
                        .field("OptFilePSize", Types.STRING)
                        .field("OptFilePModifyTime", Types.STRING)
                        .field("OptContainerId", Types.STRING)
                        .field("OptQuaratine", Types.BOOLEAN)
                        .field("SubFileName", Types.STRING)
                        .field("SubFilePath", Types.STRING)
                        .field("SubPid", Types.LONG)
                        .field("SubProcCmdline", Types.STRING)
                        .field("SubUserName", Types.STRING)
                        .field("SubUid", Types.LONG)
                        .field("SubGroupName", Types.STRING)
                        .field("SubGid", Types.LONG)
                        .field("SubProcStdin", Types.STRING)
                        .field("SubProcStdout", Types.STRING)
                        .field("SubFileSize", Types.LONG)
                        .field("SubModifyTime", Types.LONG)
                        .field("SubFileMd5", Types.STRING)
                        .field("SubFileType", Types.INT)
                        .field("SubScanResult", Types.INT)
                        .field("SubScanExt", Types.INT)
                        .field("SubScanVirusName", Types.STRING)
                        .field("SubQuaratine", Types.BOOLEAN)
                        .field("SubLIP", Types.STRING)
                        .field("SubRIP", Types.STRING)
                        .field("SubLPort", Types.INT)
                        .field("SubRPort", Types.INT)
                        .field("SubNetStatus", Types.STRING)
                        .field("SubNetProto", Types.STRING)
                        .field("LogName", Types.STRING)
                        .field("LogText", Types.STRING)
                        .field("LogOffset", Types.LONG)
                        .field("LogLoginIp", Types.STRING)
                        .field("LogLoginPort", Types.INT)
                        .field("LogLoginUserName", Types.STRING)
                        .field("LogLoginStyle", Types.STRING)
                        .field("LogLoginResult", Types.STRING)
                        .field("LogLoginCount", Types.INT)
                        .field("Platform", Types.INT)
                        .field("OsVersion", Types.STRING)
                        .field("Os6432", Types.INT)
                        .field("IPList", Types.STRING)
                        .field("EdrVersion", Types.STRING)
                        .field("UserName", Types.STRING)
                        .field("HostName", Types.STRING)
                        .field("AttackType", Types.STRING)
                        .field("OptCloudResult", Types.INT)
                        .field("OptCloudVirusName", Types.STRING)
                        .field("SubCloudResult", Types.INT)
                        .field("SubCloudVirusName", Types.STRING)
                        .field("EventId", Types.LONG)
                        .field("event_time", Types.SQL_TIMESTAMP).rowtime(new Rowtime() //定义时间时间
                                .timestampsFromField("event_timestamp")//event_timestamp 格式必须是 2019-10-24T22:18:30.000Z
                                .watermarksPeriodicBounded(60000)
                        )
                ).inAppendMode().registerTableSource("agentdata");


        //inputSQL = "select * from agentdata";
        tableEnv.registerFunction("trojanQuery", new TrojanQuery());
        Table result = tableEnv.sqlQuery(inputSQL);

        //获取查询的字段名称
        String[] fields = result.getSchema().getFieldNames();

        DataStream<Tuple2<Boolean, Row>> rowResult = tableEnv.toRetractStream(result, Row.class);

        String sql_key = sqlKey;
        DataStream<String> ds = rowResult.filter((FilterFunction<Tuple2<Boolean, Row>>) value -> value.f0).map((MapFunction<Tuple2<Boolean, Row>, String>) value -> {
            HashMap<String, Object> results = new HashMap<>();
            int index = 0;
            for (String f : fields) {
                results.put(f, value.f1.getField(index));
                index++;
            }
            results.put("sql_key", sql_key);
            return JSON.toJSON(results).toString();
        }).setParallelism(parallelNum);

        ds.addSink(new FlinkKafkaProducer011<>(kafka, "flink_result", new SimpleStringSchema())).name("flink-connectors-kafka-" + sqlKey).setParallelism(parallelNum);
        String server_info = "Query: key=" + sqlKey + "|zk=" + zk + "|kafka=" + kafka + "|sql=" + inputSQL;
        env.execute(server_info);
    }
}
