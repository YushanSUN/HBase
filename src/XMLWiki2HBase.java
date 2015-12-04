import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.DefaultHandler2;

public class XMLWiki2HBase extends DefaultHandler2 {
	private Table table;
	
	private StringBuffer currentContent=new StringBuffer();
	
	private Map<String,String> namespaces=new HashMap<String,String>();
	private String title;
	private Map<String, String> properties=new HashMap<String,String>();

	private String keyAttribute;

	public XMLWiki2HBase(Table t) {
		table=t;
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		String text=new String(ch,start,length);
		if(!text.matches("^\\s+$")) {
		  currentContent.append(ch, start, length);
		}
	}
	
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		currentContent.setLength(0);
		
		if(qName=="page") {
		  properties.clear();
		} else if(qName=="namespace") {
			keyAttribute=attributes.getValue("key");
		}
	}
	
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if(qName=="title") {
			title=currentContent.toString();
		} else if(qName=="page") {
			Put put=new Put(Bytes.toBytes(title));
			for(String key: properties.keySet()) {
				put.addColumn(Bytes.toBytes("wiki"),Bytes.toBytes(key),Bytes.toBytes(properties.get(key)));
			}
			try {
				table.put(put);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
		} else if(qName=="namespace") {
			namespaces.put(keyAttribute,currentContent.toString());
		} else {
			String value = currentContent.toString();
			if (qName == "ns") {
				value = namespaces.get(value);
			}
			if(value.length()>0)
				properties.put(qName, value);
		}
		
		currentContent.setLength(0);
	}
	
	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser = factory.newSAXParser();
		
		Configuration hadoopConfig=MyConfigurationFactory.create();
		Connection connection=ConnectionFactory.createConnection(hadoopConfig);

		XMLWiki2HBase handler=new XMLWiki2HBase(connection.getTable(TableName.valueOf("simplewiki")));
		parser.parse(new File(args[0]),handler);
		System.err.println("done");
	}
}
