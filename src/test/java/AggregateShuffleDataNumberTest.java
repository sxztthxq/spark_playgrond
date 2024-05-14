import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class AggregateShuffleDataNumberTest {
	@Test
	public void testGroupbyShuffleDataNumberTest() throws Exception {
		//System.out.println("aqe default status: " + org.apache.spark.sql.internal.SQLConf.get().getConfString("spark.sql.adaptive.enabled"));
		final int checkPartitionNumber = 10;
		SparkSession spark = SparkSession.builder()
				.config("spark.sql.adaptive.enabled", "false")
				.config("spark.sql.shuffle.partitions", checkPartitionNumber) // default: 200
				.config("spark.sql.leafNodeDefaultParallelism", "2") // source parallelism
				//.config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
				//.config("spark.sql.autoBroadcastJoinThreshold", "-1") //disable broadcast join
				.master("local")
				.getOrCreate();
		spark.sql("SET spark.sql.shuffle.partitions").show();

		final int seed = 42;
		String demoID = "0f42ad101c0e1c2243e1a0e87c600eb6f4e2a05a";
		Dataset<Row> df1 =
				spark.read().orc("/Users/xinzhou.song/Desktop/shuffle-hash/ftms_dwd_oneid.tact_dim_customer_oneid_f");
//        df1 = df1.repartition(5);
		df1.createTempView("t1");
		spark.sql("select oneid from t1 union all select oneid from t1").createOrReplaceTempView("t1");

//        spark.sql("select spark_partition_id() as mapid, oneid from t1").createOrReplaceTempView("t1");
//        spark.sql("select max(mapid) from t1").show();


//case: group by, but combine, group by因此默认存在map side combine。会使得reducer断(shuffle read)读取的行数少于这里计算的
		spark.sql("select oneid,count(1) from (select oneid from t1) group by oneid")
				.foreachPartition((ForeachPartitionFunction<Row>) t -> {
					long counter = 0;
					while (t.hasNext()) {
						counter = counter + (long) t.next().get(1);
					}
					System.out.println(String.format("Partition上的数量统计完成，共%s条", counter));
				});


//                .foreachPartition((ForeachPartitionFunction<Row>) it -> {
//                    while (it.hasNext()) {
//
//
//                        System.out.println(it.next());
//                        break;
//                    }
//                });

//        Dataset<Row> rs1 = spark.sql(String.format("select spark_partition_id() as mappid, pmod(hash(oneid), %s) as "
//				+ "partitionID,count(1),count(distinct oneid) as distinct_oneid_per_upstream_parti "
//				+ "from t1 group by mappid,partitionID order by partitionID", checkPartitionNumber, checkPartitionNumber));
//
//        rs1.createTempView("rs1");
//        rs1.show();
//        spark.sql("select partitionID, sum(distinct_oneid_per_upstream_parti) as after_combine "
//				+ "from rs1 group by partitionID").show(50);
//        spark.sql("select pmod(hash(oneid), 10) as partitionID,count(1) from t1 group by partitionID").show();
//        //spark.sql("select hash('0f42ad101c0e1c2243e1a0e87c600eb6f4e2a05a'), pmod(hash('0f42ad101c0e1c2243e1a0e87c600eb6f4e2a05a'), 200) as partitionID ").show();
//        // pmod(hash(oneid#0, 42), 200)
//        UTF8String utf8String = UTF8String.fromString(demoID);
//        int code = Murmur3_x86_32.hashUnsafeBytes(utf8String.getBaseObject(), utf8String.getBaseOffset(), utf8String.numBytes(), seed);
//        Assert.assertEquals(code, -912688121);
//        System.out.println("code == " + code);
//        int partitionNum = org.apache.spark.sql.internal.SQLConf.get().numShufflePartitions();
//        Assert.assertEquals(partitionNum, checkPartitionNumber);
//        System.out.println("aqe status: " + org.apache.spark.sql.internal.SQLConf.get().getConfString("spark.sql.adaptive.enabled"));
		TimeUnit.DAYS.sleep(1);
	}
}
