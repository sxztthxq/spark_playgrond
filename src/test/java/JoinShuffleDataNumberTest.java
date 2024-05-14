import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class JoinShuffleDataNumberTest {

	@Test
	public void testJoinShuffleDataNumberTest() throws Exception {
		//System.out.println("aqe default status: " + org.apache.spark.sql.internal.SQLConf.get().getConfString("spark.sql.adaptive.enabled"));
		SparkSession spark = SparkSession.builder()
				.config("spark.sql.adaptive.enabled", "false")
				.config("spark.sql.shuffle.partitions", "10") // default: 200
				.config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
				.config("spark.sql.autoBroadcastJoinThreshold", "-1") //disable broadcast join
				.master("local")
				.getOrCreate();


		// spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1").show();
		spark.sql("SET spark.sql.shuffle.partitions").show();

		final int seed = 42;
		String demoID = "0f42ad101c0e1c2243e1a0e87c600eb6f4e2a05a";


		Dataset<Row> left =
				spark.read().orc("/Users/xinzhou.song/Desktop/shuffle-hash/ftms_dwd_oneid.tact_dim_customer_oneid_f");
		Dataset<Row> right = spark.read()
				.orc("/Users/xinzhou.song/Desktop/shuffle-hash/ftms_dm_tact.customer_label_product_prefer_rules");
		left.createOrReplaceTempView("oneid");

//		spark.sql("select 'aa8344cd010371789eb6d81fb6f30f830076ba30' as oneid from oneid union all select oneid from oneid").createOrReplaceTempView("oneid");
//		spark.sql("select 'aa8344cd010371789eb6d81fb6f30f830076ba30' as oneid, cast(ceiling(rand() * 2) as int) as rank"
//				+ " from oneid union all select oneid, 0 as rank from oneid").createOrReplaceTempView("oneid");

//		right.createOrReplaceTempView("rules");
//		spark.sql("select oneid, 1 as rank from rules where oneid = 'aa8344cd010371789eb6d81fb6f30f830076ba30'"
//				+ "union all select oneid, 2 as rank from rules where oneid = "
//				+ "'aa8344cd010371789eb6d81fb6f30f830076ba30'"
//				+ "union all select oneid, 1 as rank from rules where oneid <> "
//				+ "'aa8344cd010371789eb6d81fb6f30f830076ba30'").createOrReplaceTempView("rules");




		//case use join not combine
//		spark.sql("select oneid.oneid, rules.oneid from oneid left join rules on oneid.oneid = rules.oneid order by "
//				+ "oneid.oneid").count();
//                .foreachPartition((ForeachPartitionFunction<Row>) it -> {
//                    while (it.hasNext()) {
//                        System.out.println(it.next());
//                        break;
//                    }
//                });


//case: group by, but combine, group by因此默认存在map side combine。会使得reducer断(shuffle read)读取的行数少于这里计算的
//        spark.sql("select oneid,count(1) from (select oneid from t1) group by oneid limit 10")
//                .foreach((ForeachFunction<Row>) row -> {
//                    System.out.println(row);
//                });


//		spark.sql("select *, cast(ceiling(rand() * 10) as int) as rank from rules").createOrReplaceTempView("rules");


        Dataset<Row> rs1 = spark.sql("select pmod(hash(oneid, rank), 10) as partitionID,count(1) cnt from oneid group by "
				+ "partitionID order by partitionID");
        rs1.createOrReplaceTempView("rs1");
        Dataset<Row> rs2 = spark.sql("select pmod(hash(oneid, rank), 10) as partitionID,count(1) cnt from rules group by "
				+ "partitionID order by partitionID");
        rs2.createOrReplaceTempView("rs2");
        spark.sql("select rs1.partitionID, rs1.cnt, rs2.cnt, rs1.cnt+rs2.cnt from rs1 join rs2 on rs1.partitionID = rs2.partitionID order by rs1.partitionID").show();
//
//        //spark.sql("select pmod(hash(oneid || '___'), 10) as partitionID,count(1) from t1 group by partitionID").show();
//        //spark.sql("select hash('0f42ad101c0e1c2243e1a0e87c600eb6f4e2a05a'), pmod(hash('0f42ad101c0e1c2243e1a0e87c600eb6f4e2a05a'), 200) as partitionID ").show();
//        // pmod(hash(oneid#0, 42), 200)
//        UTF8String utf8String = UTF8String.fromString(demoID);
//        int code = Murmur3_x86_32.hashUnsafeBytes(utf8String.getBaseObject(), utf8String.getBaseOffset(), utf8String.numBytes(), seed);
//        Assert.assertEquals(code, -912688121);
//        System.out.println("code == " + code);
//        //int partitionNum = org.apache.spark.sql.internal.SQLConf.get().numShufflePartitions();
//        //Assert.assertEquals(partitionNum, 200);
//        System.out.println("aqe status: " + org.apache.spark.sql.internal.SQLConf.get().getConfString("spark.sql.adaptive.enabled"));
		TimeUnit.DAYS.sleep(1);
	}

}
