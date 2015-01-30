import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Iterables;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class Sparky {

	/*helper function which computes sum of the incoming pageranks*/
	@SuppressWarnings("serial")
	private static class Sum implements Function2<Double, Double, Double> {
		@Override
		public Double call(Double a, Double b) {
			return a + b;
		}
	}
	
	
	static double danglingContrib;
	static HashSet<String> dangUrls = new HashSet<String>();

	@SuppressWarnings("serial")
	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf().setAppName("sparkPageRank");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		StringBuilder inputPath = new StringBuilder();
	     
	    for(int i=0;i<10;i++)
	    {
	    	inputPath.append("s3n://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-0000"+i+",");
	    
	    }	
	    for(int i=10;i<100;i++)
	    {
	    	inputPath.append("s3n://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-000"+i+",");	
		}
	    for(int i=100;i<300;i++)
	    {
	    inputPath.append("s3n://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-00"+i+",");	
	    }	
	   	
	    inputPath.append("s3n://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-00300");
	    
		
	    JavaPairRDD<Text, Text> entries= ctx.sequenceFile(inputPath.toString(), Text.class, Text.class);
	    
	    /* Converts Text Tuples to String Tuples  */
	    JavaPairRDD<String, String> lines = entries.mapToPair(new PairFunction<Tuple2<Text,Text>,String,String>() {
	    	@Override
			public Tuple2<String, String> call(Tuple2<Text, Text> arg0)
					throws Exception {
				String url = arg0._1.toString();
				String json = arg0._2.toString();
				Tuple2<String,String> oneObj=new Tuple2<String,String>(url,json);
				return oneObj;
				
			}
			
	    });
	    
	    
	    JavaPairRDD<String, Iterable<String>> links = lines.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
			
			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, String> arg0)
					throws Exception {

				String url = arg0._1;
				ArrayList<Tuple2<String,String>> values = new ArrayList<Tuple2<String,String>>();
				
				JsonElement jelement = new JsonParser().parse(arg0._2);
				JsonObject  jobjectContent = jelement.getAsJsonObject();
				jobjectContent = jobjectContent.getAsJsonObject("content");
				boolean isDangling=true;;
				if (jobjectContent!=null)
				{
					JsonArray jarrayLinks = jobjectContent.getAsJsonArray("links");
					if (jarrayLinks!=null)
					{
						int size = jarrayLinks.size();
						
						for(int i=0;i<size;i++)
						{
							JsonObject temp = jarrayLinks.get(i).getAsJsonObject();
							String aLink = temp.get("href").toString();
							String type = temp.get("type").toString();
							if (type.equals("\"a\""))
							{	
								aLink = aLink.replace("\"", "");
								isDangling = false;
								values.add(new Tuple2<String,String>(url,aLink));
								
							}
						}
					}

				}
				if(isDangling) 
				{
					values.add(new Tuple2<String,String>(url,null));
					dangUrls.add(url);
				}
				return (values);
		}
				
				
				
			}).distinct().groupByKey().cache();


		List<String> keys = links.keys().collect();
		
		HashSet<String> allKeys = new HashSet<String>();
		for(String s : keys) {
			allKeys.add(s);
			
		}

		final Broadcast<HashSet<String>> keySet = ctx.broadcast(allKeys);

		JavaPairRDD<String, Iterable<String>> allUrls = links.flatMapToPair
				(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, Iterable<String>>() {

					@Override
					public Iterable<Tuple2<String, Iterable<String>>> call(
							Tuple2<String, Iterable<String>> arg)
									throws Exception {
						List<Tuple2<String, Iterable<String>>> results = new ArrayList<Tuple2<String, Iterable<String>>>();
						if(arg._2 != null)
							for(String url:arg._2) {
								if(!(keySet.value().contains(url)) && url != null) {
									dangUrls.add(url);
									results.add(new Tuple2<String, Iterable<String>>(url,null));
								}
							}
						else //Didn't had any outgoing urls in the initial keyset
						{
							dangUrls.add(arg._1);
							
						}
						return results;
					}
				}).distinct().cache();

		allUrls = links.union(allUrls);	
		final Broadcast<Long> totalUrlCount = ctx.broadcast(allUrls.count()); 

		// Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
		JavaPairRDD<String, Double> ranks = allUrls.mapValues(new Function<Iterable<String>, Double>() {
			@Override
			public Double call(Iterable<String> rs) {
				return 1.0;
			}
		}).cache();

		HashSet<String> notDanglingButStillInDangUrl = new HashSet<String>();
		
		for(String s : dangUrls){
			if(!(allUrls.lookup(s) == null || allUrls.lookup(s).isEmpty() || (allUrls.lookup(s).size() == 1 && allUrls.lookup(s).get(0) == null))) {
				
				notDanglingButStillInDangUrl.add(s);
			}
		}

		
		for(String s : notDanglingButStillInDangUrl){
			dangUrls.remove(s);
		}

		// Calculates and updates URL ranks continuously using PageRank algorithm.
		for (int iter = 0; iter < 10; iter++) {
			System.out.println("Starting iter"+iter);
			danglingContrib = 0;
			
			// Calculates URL contributions to the rank of other URLs.
			JavaPairRDD<String, Double> contribs = allUrls.join(ranks).values()
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
						@Override
						public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> arg0) {
							int urlCount = 0;
							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
							if(arg0._1 != null) {
								urlCount = Iterables.size(arg0._1);
								for (String s : arg0._1) {
									if(s == null) {
										if(urlCount == 1){
										}
										urlCount--;
									}
								}
								Double pageRank = arg0._2() / urlCount;
								for (String s : arg0._1) {
									if(s != null)          		
										results.add(new Tuple2<String, Double>(s,pageRank ));
									
								}
							}
							return results;
						}
					}).cache();

			
			for(String url: dangUrls) {
				danglingContrib += ranks.lookup(url).get(0);
				
			}

			JavaPairRDD<String, Double> xclusiveElements = ranks.subtractByKey(contribs);
			contribs = contribs.union(xclusiveElements);


			// Re-calculates URL ranks based on neighbor contributions.
			ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
				@Override
				public Double call(Double sum) {

					return 0.15 + 0.85 * (sum + danglingContrib/(double)totalUrlCount.value().longValue());
				}
			}).cache();
			
			ranks.saveAsTextFile("s3n://mayurs719project2/PageRank"+iter+"/");
		}
		
		
		
		ctx.stop();


	}    
}



