package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import org.apache.flink.types.Row;

public class FlinkReadWriteKafkaJSON {
        public static void main(String[] args) throws Exception {
            // Read parameters from command line
            final ParameterTool params = ParameterTool.fromArgs(args);

            if(params.getNumberOfParameters() < 4) {
                System.out.println("\nUsage: FlinkReadKafka " +
                                   "--read-topic <topic> " +
                                   "--write-topic <topic> " +
                                   "--bootstrap.servers <kafka brokers> " +
                                   "--group.id <groupid>");
                return;
            }

            // define a schema
            String[] fieldNames = { "flight", "timestamp_verbose", "msg_type", "track",
                    "timestamp", "altitude", "counter", "lon",
                    "icao", "vr", "lat", "speed" };
            TypeInformation<?>[] dataTypes = { Types.INT, Types.SQL_TIMESTAMP, Types.STRING, Types.STRING,
                    Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.STRING,
                    Types.STRING, Types.STRING, Types.STRING, Types.STRING };

            TypeInformation<Row> dataRow = Types.ROW_NAMED(fieldNames, dataTypes);

            // setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            KafkaTableSource kafkaTableSource = Kafka010JsonTableSource.builder()
                    .forTopic(params.getRequired("read-topic"))
                    .withKafkaProperties(params.getProperties())
                    .withSchema(TableSchema.fromTypeInfo(dataRow))
                    .forJsonSchema(TableSchema.fromTypeInfo(dataRow))
                    .build();

            String sql = "SELECT flight, " +
                         "TUMBLE_START(timestamp_verbose, INTERVAL '1' DAY) as wStart,  " +
                         "max(altitude) " +
                         "FROM flights " +
                         "WHERE altitude <> '' " +
                         "GROUP BY TUMBLE_START(timestamp_verbose, INTERVAL '1' DAY), flight";


            tableEnv.registerTableSource("flights", kafkaTableSource);
            Table result = tableEnv.sqlQuery(sql);


            // create a partition for the data going into kafka
            FlinkFixedPartitioner partition =  new FlinkFixedPartitioner();

            // create new tablesink of JSON to kafka
            KafkaJsonTableSink kafkaTableSink = new Kafka09JsonTableSink(
                    params.getRequired("write-topic"),
                    params.getProperties(),
                    partition);

            result.writeToSink(kafkaTableSink);

            env.execute("FlinkReadWriteKafkaJSON");
        }
}

