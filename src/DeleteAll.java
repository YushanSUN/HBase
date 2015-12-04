import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;

public class DeleteAll {
	public static void main(String[] args) throws IOException {
		Configuration hadoopConfig = MyConfigurationFactory.create();
		Connection connection = ConnectionFactory
				.createConnection(hadoopConfig);
		Table table = connection.getTable(TableName.valueOf(InvertedIndex.getLogin()));

		Scan scan = new Scan();

		ResultScanner scanner = table.getScanner(scan);

		List<Delete> deletes = new ArrayList<Delete>();
		int bufferSize = 10000000;
		int counter = 0;

		Result result;
		while ((result = scanner.next()) != null) {
			if (counter < bufferSize) {
				deletes.add(new Delete(result.getRow()));
				counter++;
			} else {
				table.delete(deletes);
				deletes.clear();
				counter = 0;
			}
		}

		if (deletes.size() > 0) {
			table.delete(deletes);
			deletes.clear();
		}
	}
}
