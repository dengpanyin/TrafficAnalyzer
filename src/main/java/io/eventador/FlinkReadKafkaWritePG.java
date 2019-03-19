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
import org.apache.flink.api.java.io.jdbc.*;
import org.apache.flink.types.Row;

public class FlinkReadKafkaWritePG {
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

        // setup streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);

        // setup the table environment
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // define the source schema
        String[] fieldNames = { "flight", "timestamp_verbose", "msg_type", "track",
                "timestamp", "altitude", "counter", "lon",
                "icao", "vr", "lat", "speed" };
        TypeInformation<?>[] dataTypes = { Types.INT, Types.SQL_TIMESTAMP, Types.STRING, Types.STRING,
                Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING, Types.STRING };

        TypeInformation<Row> dataRow = Types.ROW_NAMED(fieldNames, dataTypes);

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

        // flight, timestamp, altitude

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                .setDBUrl("jdbc:derby:memory:ebookshop")
                .setQuery("INSERT INTO planes (flight, timestamp, altitude) VALUES (?, ?, ?)")
                .setParameterTypes(Types.STRING, Types.SQL_TIMESTAMP, Types.STRING)
                .build();


        tableEnv.sqlUpdate(sql);




        env.execute("FlinkReadWriteKafkaJSON");
    }
}

