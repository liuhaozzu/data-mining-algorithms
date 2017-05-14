package com.liuhaozzu.mr.weibo.first;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import com.sun.jersey.client.impl.CopyOnWriteHashMap;

/**
 * 第一个MR,计算TF和计算N（微博总数）
 * 
 * @author Administrator
 *
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Log LOGGER = LogFactory.getLog(FirstMapper.class);
	public static final Map<LongWritable, Text> MAP = new CopyOnWriteHashMap<>();

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] v = ivalue.toString().trim().split("\t");
		LOGGER.info("ikey>>>" + ikey);
		LOGGER.info("ivalue>>>" + ivalue);
		MAP.put(ikey, ivalue);
		if (v.length >= 2) {
			String id = v[0].trim();
			String content = v[1].trim();
			LOGGER.info("id>>>" + id + ";content>>>" + content);

			StringReader sr = new StringReader(content);
			IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
			Lexeme word;
			while ((word = ikSegmenter.next()) != null) {
				String w = word.getLexemeText();
				context.write(new Text(w + "_" + id), new IntWritable(1));
			}
			context.write(new Text("count"), new IntWritable(1));
		} else {
			LOGGER.info(ivalue.toString() + "------------------");
		}
	}

}
