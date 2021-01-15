package com.datax.dataset.connectors.hbase;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


/**
 *
 */
public class TestReadFromHBase {
    public static final byte[] family = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);


    public static void main(String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, Integer, String>> users = env.createInput(

                /**
                 *  TableInputFormat
                 */
                new TableInputFormat<Tuple4<String, String, Integer, String>>() {

                    @Override
                    public void configure(org.apache.flink.configuration.Configuration parameters) {
                        table = createTable();
                        if (table != null) {
                            scan = getScanner();
                        }
                    }


                    private HTable createTable() {
                        System.out.println("Initializing HBaseConfiguration");
                        //use files found in the classpath
                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

                        conf.set("hbase.zookeeper.quorum", "slave01,slave02,slave03");
                        conf.set("hbase.zookeeper.property.clientPort", "2181");
                        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

                        try {
                            return new HTable(conf, getTableName());
                        } catch (Exception e) {
                            System.out.println("Error instantiating a new HTable instance" + e.getMessage());
                        }
                        return null;
                    }

                    @Override
                    protected Scan getScanner() {
                        Scan scan = new Scan();
                        scan.addFamily(family);
                        return scan;
                    }

                    @Override
                    protected String getTableName() {
                        return "learing_flink:users";
                    }

                    @Override
                    protected Tuple4<String, String, Integer, String> mapResultToTuple(Result r) {
                        // 把一行数据，转为 Tuple4
                        return Tuple4.of(
                                Bytes.toString(r.getRow()),
                                Bytes.toString(r.getValue(family, "name".getBytes(ConfigConstants.DEFAULT_CHARSET))),
                                Integer.parseInt(
                                        Bytes.toString(
                                                r.getValue(
                                                        family, "age".getBytes(
                                                                ConfigConstants.DEFAULT_CHARSET
                                                        )
                                                )
                                        )
                                ),
                                Bytes.toString(r.getValue(family, "address".getBytes(ConfigConstants.DEFAULT_CHARSET)))
                        );
                    }
                });

        users.print();

    }
}
