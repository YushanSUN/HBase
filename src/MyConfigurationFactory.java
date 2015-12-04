import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class MyConfigurationFactory {
	private MyConfigurationFactory() {
	}

	static Configuration create() {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "lame14.enst.fr,lame15.enst.fr");
		config.set("fs.default.name", "hdfs://lame11.enst.fr/");
		return config;
	}
}
