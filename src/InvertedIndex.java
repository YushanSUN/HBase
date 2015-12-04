import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndex {
	public static final byte indexFamily[] = Bytes.toBytes("index");
	private static Configuration configuration = MyConfigurationFactory
			.create();

	private static enum COUNTER {
		NB_DOCUMENTS
	};
	private static final String tableName = "yusun";
	
	static class Map extends TableMapper<Text, PairTextDoubleWritable> {
		// Note that this time, only letters, not digits, are accepted within
		// tokens
		private static final Pattern alpha = Pattern.compile("[\\p{L}]+");

		private HashSet<String> stopWords;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			stopWords = new HashSet<String>();
			retrieveStopWords("/user/senellar/stopWords.txt");
		}

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			String key = new String(row.get());

			// TODO: filter documents from the non-default namespace
			if(!key.contains(":")){
				TreeMap<String, Integer> count = new TreeMap<String, Integer>();
	
				int length = 0;
	
				Stemmer s = new Stemmer();
	
				String v = null;
				
				// TODO: extract appropriate cell from value into v 
				for(Cell cell : value.getColumnCells(Bytes.toBytes("wiki"), Bytes.toBytes("text"))){
			          v = Bytes.toString(CellUtil.cloneValue(cell));
			        }      
				Matcher m = alpha.matcher(v);
	
				while (m.find()) {
					String token = s.stem(m.group().toLowerCase());
					if (isStopWord(token))
						continue;
					Integer tokenCount = count.get(token);
					tokenCount = (tokenCount == null ? 1 : tokenCount + 1);
					count.put(token, tokenCount);
					++length;
				}
	
				for (String token : count.keySet()) {
					context.write(new Text(token), new PairTextDoubleWritable(
							new Text(key), new DoubleWritable(count.get(token) * 1.
									/ length)));
				}
				
				// TODO: increase the NB_DOCUMENTS counter
				Counter counter = context.getCounter(COUNTER.NB_DOCUMENTS);
			      counter.increment(1);
			}			
		}

		public void retrieveStopWords(String filename) {
			Stemmer s = new Stemmer();

			try {
				//BufferedReader r = null;
				
				// TODO: open and read the file; filename is here an HDFS file name
				FileSystem fs = FileSystem.get(configuration);
	              BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(new Path(filename))));
	    
				String line;
				while ((line = r.readLine()) != null) {
					stopWords.add(s.stem(line));
				}
				r.close();
			} catch (IOException e) {
				System.err.println("Cannot load stop words.");
				e.printStackTrace();
				System.exit(0);
			}
		}

		public boolean isStopWord(String token) {
			return stopWords.contains(token);
		}
	}

	static class Reduce extends
			Reducer<Text, PairTextDoubleWritable, ImmutableBytesWritable, Cell> {
		private Counter nb_documents;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Cluster c = new Cluster(context.getConfiguration());
			nb_documents = c.getJob(context.getJobID()).getCounters()
					.findCounter(COUNTER.NB_DOCUMENTS);
		}

		@Override
		protected void reduce(Text key,
				Iterable<PairTextDoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			/*
			 * Storing in main memory because we cannot iterate twice and we
			 * need the number of records
			 */
			TreeSet<DocumentWeight> s = new TreeSet<DocumentWeight>();

			for (PairTextDoubleWritable val : values) {
				s.add(new DocumentWeight(val.getFirst().toString(), val
						.getSecond().get()));
			}

			double idf = Math.log(nb_documents.getValue() * 1. / s.size())
					/ Math.log(2);

			for (DocumentWeight dw : s) {
				KeyValue cell = null;
				
				// TODO: Produce a KeyValue object with row key, family, qualifier, value
				cell = new KeyValue(Bytes.toBytes(key.toString()),Bytes.toBytes("index"),
			            Bytes.toBytes(dw.documentTitle),Bytes.toBytes(dw.weight * idf));
				context.write(
						new ImmutableBytesWritable(
								Bytes.toBytes(key.toString())), cell);
			}
		}
	}

	public static void buildInvertedIndex(String input, String output)
			throws Exception {
		Job job = Job.getInstance(configuration);
		job.setJarByClass(InvertedIndex.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		// TODO: add columns to scan that should be retrieved
		//scan.addColumn(Bytes.toBytes("simplewiki"), Bytes.toBytes("text"));
		
		TableMapReduceUtil.initTableMapperJob(input, scan, Map.class,
				Text.class, PairTextDoubleWritable.class, job);
		job.setReducerClass(Reduce.class);

		job.setOutputFormatClass(HFileOutputFormat2.class);

		@SuppressWarnings("deprecation")
		HTable outputTable = new HTable(configuration, output);
		//Path outputPath = null;
		
        // TODO: set the outputPath variable to an HDFS location where the produced file
        // will be stored
		//FileSystem fs = FileSystem.get(configuration);
		Path outputPath = new Path("/user/yusun/hbase/"+System.currentTimeMillis());
		//fs.delete(outputPath, true);
		
		HFileOutputFormat2.setOutputPath(job, outputPath);

		job.setNumReduceTasks(2);
		job.waitForCompletion(true);

		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
		loader.doBulkLoad(outputPath, outputTable);
	}

	public static class DocumentWeight implements Comparable<DocumentWeight> {
		String documentTitle;
		double weight;

		DocumentWeight(String t, double d) {
			documentTitle = t;
			weight = d;
		}

		@Override
		public int compareTo(DocumentWeight that) {
			return this.documentTitle.compareTo(that.documentTitle);
		}
	}

	public static void main(String[] args) throws Exception {
		buildInvertedIndex("simplewiki", getLogin());
	}

	public static String getLogin() {
		// TODO: return a constant string formed of your login
		//String login = "yusun";    
		   return tableName;
	}

	private Table table;

	InvertedIndex() throws IOException {
		// TODO: put a reference to the HBase table where the inverted index
        // is stored in the table class field
		 HConnection connection = HConnectionManager.createConnection(configuration);
		   table = connection.getTable(tableName);
	}

	public double getScore(String token, String document) throws IOException {
		// TODO: get the score of this token in this document from the inverted index;
		// return -1 if the token does not occur in the document
		double score=-1;
		   String file="index";
		   if (table!=null){
		    Get get = new Get(Bytes.toBytes(token));
		     get.addColumn(Bytes.toBytes(file),Bytes.toBytes(document));
		     Result res = table.get(get);
		     if(!res.isEmpty())
		       score=Bytes.toDouble(res.getValue(Bytes.toBytes(file), Bytes.toBytes(document)));
		     return score;
		   }
		  return score;
	}

	public int getNumberColumns(String token) throws IOException {
		// TODO: return the size of the posting list for this token
		int numCol=0;
	    Get get = new Get(Bytes.toBytes(token));
	    Result res = table.get(get);
	    if(!res.isEmpty())
	      numCol=res.size();
	    return numCol;
	}
}