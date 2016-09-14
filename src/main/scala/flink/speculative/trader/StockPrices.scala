package flink.speculative.trader



import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

import java.io.Serializable
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector


object StockPrices {
  private val SYMBOLS: Seq[String] = Seq("SPX", "FTSE", "DJI", "DJT", "BUX", "DAX", "GOOG")
  private val DEFAULT_PRICE: Double = 1000.0
  //XXX private val DEFAULT_STOCK_PRICE: StockPrices.StockPrice = new StockPrices.StockPrice("", DEFAULT_PRICE)
}
class StockPrices(symbol: String, default_price: Double) {
  val env = StreamExecutionEnvironment.getExecutionEnvironment()
  //XXX val text: DataStream[String] = env.readTextFile("file:///path/to/file")


}


case class StockPrice(val symbol: String, val price: Double) {
  override def toString: String = "StockPrice{" + "symbol='" + symbol + '\'' + ", count=" + price + '}'
}

case class Count(val symbol: String, val count: Int) {
  override def toString: String = "Count{" + "symbol='" + symbol + '\'' + ", count=" + count + '}'
}

final class StockSource(val symbol: String, val sigma: Int) extends RichSourceFunction[StockPrice] {
  import scala.util.Random
  import org.apache.flink.configuration.Configuration
  import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

  private val random: Random = new Random

  private var price: Double = 1000.0 //TODO: DEFAULT_PRICE

  override def open(parameters: Configuration) {
    super.open(parameters)
  }

  private def reachedEnd(): Boolean = false

  private def next(): StockPrice = {
    price = price + random.nextGaussian * sigma
    Thread.sleep(random.nextInt(200))
    new StockPrice(symbol, price)
  }

  //TODO: study API and find how to define this function
  override def cancel(): Unit = ???

  //TODO: study API and find how to define this function
  override def run(ctx: SourceContext[StockPrice]): Unit = ???
}


final class TweetSource(val symbols: Seq[String]) extends RichSourceFunction[String] {
  //TODO
}









// 	public static final class TweetSource extends RichSourceFunction<String> {
//
// 		private static final long serialVersionUID = 1L;
// 		private transient Random random;
// 		private transient StringBuilder stringBuilder;
//
// 		@Override
// 		public void open(Configuration parameters) throws Exception {
// 			super.open(parameters);
// 			random = new Random();
// 			stringBuilder = new StringBuilder();
// 		}
//
// 		@Override
// 		public boolean reachedEnd() throws Exception {
// 			return false;
// 		}
//
// 		@Override
// 		public String next() throws Exception {
// 			stringBuilder.setLength(0);
// 			for (int i = 0; i < 3; i++) {
// 				stringBuilder.append(" ");
// 				stringBuilder.append(SYMBOLS.get(random.nextInt(SYMBOLS.size())));
// 			}
// 			Thread.sleep(500);
// 			return stringBuilder.toString();
// 		}
//
// 	}


object X {
  def mean(values: Seq[StockPrice]): StockPrice = {
    keyedStream.fold(0.0)((values, i) => { values + "-" + i })
}

}


class WindowMean extends RichMapFunction[Seq[StockPrice], StockPrice] {
  override def map(values: Seq[StockPrice]): StockPrice = {
    new StockPrice(values(0).symbol, values.map(value => value.price).sum / values.size)
  }
}


class WindowCorrelation extends RichMapFunction[Seq[(Int, Int)], Double] {
  override def map(values: Seq[(Int, Int)]): Double = {
    val count  = values.size
    // sum
    val lSum   = values.map { case (l, _) => l }.sum
    val rSum   = values.map { case (_, r) => r }.sum
    // mean
    val lMean  = lSum.doubleValue() / count
    val rMean = rSum.doubleValue() / count
    // stddev
    val lStdDev = (0.0 /: values) { case (acc, (l, _)) => acc + Math.pow(l - lMean, 2) / count }
    val rStdDev = (0.0 /: values) { case (acc, (_, r)) => acc + Math.pow(r - rMean, 2) / count }
    // covariance
    val cov = (0.0 /: values) { case (acc, (l, r)) => acc + ((l -lMean) * (r - rMean) / count) }
    // correlation
    cov / (lStdDev * rStdDev)
  }
}


//---------------------------------------------------------------------------------------------------------------------

// /*
//  * Licensed to the Apache Software Foundation (ASF) under one or more
//  * contributor license agreements.  See the NOTICE file distributed with
//  * this work for additional information regarding copyright ownership.
//  * The ASF licenses this file to You under the Apache License, Version 2.0
//  * (the "License"); you may not use this file except in compliance with
//  * the License.  You may obtain a copy of the License at
//  *
//  *    http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
//
// package flink.speculative.trader;
//
// import java.io.Serializable;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Random;
// import java.util.concurrent.TimeUnit;
//
// import org.apache.flink.api.common.functions.FilterFunction;
// import org.apache.flink.api.common.functions.FlatMapFunction;
// import org.apache.flink.api.common.functions.JoinFunction;
// import org.apache.flink.api.common.functions.MapFunction;
// import org.apache.flink.api.java.tuple.Tuple2;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.datastream.WindowedDataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.api.functions.WindowMapFunction;
// import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
// import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
// import org.apache.flink.streaming.api.windowing.helper.Delta;
// import org.apache.flink.streaming.api.windowing.helper.Time;
// import org.apache.flink.util.Collector;
//
// /**
//  * This example showcases a moderately complex Flink Streaming pipeline.
//  * It to computes statistics on stock market data that arrive continuously,
//  * and combines the stock market data with tweet streams.
//  * For a detailed explanation of the job, check out the blog post unrolling it.
//  * To run the example make sure that the service providing the text data
//  * is already up and running.
//  *
//  * <p>
//  * To start an example socket text stream on your local machine run netcat from
//  * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
//  * port number.
//  *
//  *
//  * <p>
//  * Usage:
//  * <code>StockPrices &lt;hostname&gt; &lt;port&gt; &lt;result path&gt;</code>
//  * <br>
//  *
//  * <p>
//  * This example shows how to:
//  * <ul>
//  * <li>union and join data streams,
//  * <li>use different windowing policies,
//  * <li>define windowing aggregations.
//  * </ul>
//  *
//  * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
//  * @see <a href="http://flink.apache.org/news/2015/02/09/streaming-example.html">blogpost</a>
//  */
// public class StockPrices {
//
// 	private static final ArrayList<String> SYMBOLS = new ArrayList<String>(Arrays.asList("SPX", "FTSE", "DJI", "DJT", "BUX", "DAX", "GOOG"));
// 	private static final Double DEFAULT_PRICE = 1000.;
// 	private static final StockPrice DEFAULT_STOCK_PRICE = new StockPrice("", DEFAULT_PRICE);
//
// 	// *************************************************************************
// 	// PROGRAM
// 	// *************************************************************************
//
// 	@SuppressWarnings({ "serial", "unused" })
// 	public static void main(String[] args) throws Exception {
//
// 		if (!parseParameters(args)) {
// 			return;
// 		}
//
// 		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
// 		//Step 1
// 	    //Read a stream of stock prices from different sources and union it into one stream
//
// 		//Read from a socket stream at map it to StockPrice objects
// 		DataStream<StockPrice> socketStockStream = env.socketTextStream(hostName, port)
// 				.map(new MapFunction<String, StockPrice>() {
// 					private String[] tokens;
//
// 					@Override
// 					public StockPrice map(String value) throws Exception {
// 						tokens = value.split(",");
// 						return new StockPrice(tokens[0], Double.parseDouble(tokens[1]));
// 					}
// 				});
//
// 		//Generate other stock streams
// 		DataStream<StockPrice> SPX_stream = env.addSource(new StockSource("SPX", 10));
// 		DataStream<StockPrice> FTSE_stream = env.addSource(new StockSource("FTSE", 20));
// 		DataStream<StockPrice> DJI_stream = env.addSource(new StockSource("DJI", 30));
// 		DataStream<StockPrice> BUX_stream = env.addSource(new StockSource("BUX", 40));
//
// 		//Merge all stock streams together
// 		@SuppressWarnings("unchecked")
// 		DataStream<StockPrice> stockStream = socketStockStream.union(SPX_stream, FTSE_stream, DJI_stream, BUX_stream);
//
// 		//Step 2
// 	    //Compute some simple statistics on a rolling window
// 		WindowedDataStream<StockPrice> windowedStream = stockStream
// 				.window(Time.of(10, TimeUnit.SECONDS))
// 				.every(Time.of(5, TimeUnit.SECONDS));
//
// 		DataStream<StockPrice> lowest = windowedStream.minBy("price").flatten();
// 		DataStream<StockPrice> maxByStock = windowedStream.groupBy("symbol").maxBy("price").flatten();
// 		DataStream<StockPrice> rollingMean = windowedStream.groupBy("symbol").mapWindow(new WindowMean()).flatten();
//
// 		//Step 3
// 		//Use  delta policy to create price change warnings, and also count the number of warning every half minute
//
// 		DataStream<String> priceWarnings = stockStream.groupBy("symbol")
// 				.window(Delta.of(0.05, new DeltaFunction<StockPrice>() {
// 					@Override
// 					public double getDelta(StockPrice oldDataPoint, StockPrice newDataPoint) {
// 						return Math.abs(oldDataPoint.price - newDataPoint.price);
// 					}
// 				}, DEFAULT_STOCK_PRICE))
// 				.mapWindow(new SendWarning()).flatten();
//
//
// 		DataStream<Count> warningsPerStock = priceWarnings.map(new MapFunction<String, Count>() {
// 			@Override
// 			public Count map(String value) throws Exception {
// 				return new Count(value, 1);
// 			}
// 		}).groupBy("symbol").window(Time.of(30, TimeUnit.SECONDS)).sum("count").flatten();
//
// 		//Step 4
// 		//Read a stream of tweets and extract the stock symbols
// 		DataStream<String> tweetStream = env.addSource(new TweetSource());
//
// 		DataStream<String> mentionedSymbols = tweetStream.flatMap(new FlatMapFunction<String, String>() {
// 			@Override
// 			public void flatMap(String value, Collector<String> out) throws Exception {
// 				String[] words = value.split(" ");
// 				for (String word : words) {
// 					out.collect(word.toUpperCase());
// 				}
// 			}
// 		}).filter(new FilterFunction<String>() {
// 			@Override
// 			public boolean filter(String value) throws Exception {
// 				return SYMBOLS.contains(value);
// 			}
// 		});
//
// 		DataStream<Count> tweetsPerStock = mentionedSymbols.map(new MapFunction<String, Count>() {
// 			@Override
// 			public Count map(String value) throws Exception {
// 				return new Count(value, 1);
// 			}
// 		}).groupBy("symbol")
// 				.window(Time.of(30, TimeUnit.SECONDS))
// 				.sum("count").flatten();
//
// 		//Step 5
// 		//For advanced analysis we join the number of tweets and the number of price change warnings by stock
// 		//for the last half minute, we keep only the counts. We use this information to compute rolling correlations
// 		//between the tweets and the price changes
//
// 		DataStream<Tuple2<Integer, Integer>> tweetsAndWarning = warningsPerStock.join(tweetsPerStock)
// 				.onWindow(30, TimeUnit.SECONDS)
// 				.where("symbol")
// 				.equalTo("symbol")
// 				.with(new JoinFunction<Count, Count, Tuple2<Integer, Integer>>() {
// 					@Override
// 					public Tuple2<Integer, Integer> join(Count first, Count second) throws Exception {
// 						return new Tuple2<Integer, Integer>(first.count, second.count);
// 					}
// 				});
//
// 		DataStream<Double> rollingCorrelation = tweetsAndWarning
// 				.window(Time.of(30, TimeUnit.SECONDS))
// 				.mapWindow(new WindowCorrelation()).flatten();
//
// 		if (fileOutput) {
// 			rollingCorrelation.writeAsText(outputPath, 1);
// 		} else {
// 			rollingCorrelation.print();
// 		}
//
// 		env.execute("Stock stream");
//
// 	}
//
// 	// *************************************************************************
// 	// DATA TYPES
// 	// *************************************************************************
//
// 	public static class StockPrice implements Serializable {
//
// 		private static final long serialVersionUID = 1L;
// 		public String symbol;
// 		public Double price;
//
// 		public StockPrice() {
// 		}
//
// 		public StockPrice(String symbol, Double price) {
// 			this.symbol = symbol;
// 			this.price = price;
// 		}
//
// 		@Override
// 		public String toString() {
// 			return "StockPrice{" +
// 					"symbol='" + symbol + '\'' +
// 					", count=" + price +
// 					'}';
// 		}
// 	}
//
// 	public static class Count implements Serializable{
//
// 		private static final long serialVersionUID = 1L;
// 		public String symbol;
// 		public Integer count;
//
// 		public Count() {
// 		}
//
// 		public Count(String symbol, Integer count) {
// 			this.symbol = symbol;
// 			this.count = count;
// 		}
//
// 		@Override
// 		public String toString() {
// 			return "Count{" +
// 					"symbol='" + symbol + '\'' +
// 					", count=" + count +
// 					'}';
// 		}
// 	}
//
// 	// *************************************************************************
// 	// USER FUNCTIONS
// 	// *************************************************************************
//
// 	public final static class StockSource extends RichSourceFunction<StockPrice> {
//
// 		private static final long serialVersionUID = 1L;
// 		private Double price;
// 		private String symbol;
// 		private Integer sigma;
// 		private transient Random random;
//
//
//
// 		public StockSource(String symbol, Integer sigma) {
// 			this.symbol = symbol;
// 			this.sigma = sigma;
// 			price = DEFAULT_PRICE;
//
// 		}
//
// 		@Override
// 		public void open(Configuration parameters) throws Exception {
// 			super.open(parameters);
// 			random = new Random();
//
// 		}
//
// 		@Override
// 		public boolean reachedEnd() throws Exception {
// 			return false;
// 		}
//
// 		@Override
// 		public StockPrice next() throws Exception {
// 			price = price + random.nextGaussian() * sigma;
// 			Thread.sleep(random.nextInt(200));
// 			return new StockPrice(symbol, price);
// 		}
//
// 	}
//
// 	public final static class WindowMean implements WindowMapFunction<StockPrice, StockPrice> {
//
// 		private static final long serialVersionUID = 1L;
// 		private Double sum = 0.0;
// 		private Integer count = 0;
// 		private String symbol = "";
//
// 		@Override
// 		public void mapWindow(Iterable<StockPrice> values, Collector<StockPrice> out) throws Exception {
// 			if (values.iterator().hasNext()) {
//
// 				for (StockPrice sp : values) {
// 					sum += sp.price;
// 					symbol = sp.symbol;
// 					count++;
// 				}
// 				out.collect(new StockPrice(symbol, sum / count));
// 			}
// 		}
// 	}
//
// 	public static final class TweetSource extends RichSourceFunction<String> {
//
// 		private static final long serialVersionUID = 1L;
// 		private transient Random random;
// 		private transient StringBuilder stringBuilder;
//
// 		@Override
// 		public void open(Configuration parameters) throws Exception {
// 			super.open(parameters);
// 			random = new Random();
// 			stringBuilder = new StringBuilder();
// 		}
//
// 		@Override
// 		public boolean reachedEnd() throws Exception {
// 			return false;
// 		}
//
// 		@Override
// 		public String next() throws Exception {
// 			stringBuilder.setLength(0);
// 			for (int i = 0; i < 3; i++) {
// 				stringBuilder.append(" ");
// 				stringBuilder.append(SYMBOLS.get(random.nextInt(SYMBOLS.size())));
// 			}
// 			Thread.sleep(500);
// 			return stringBuilder.toString();
// 		}
//
// 	}
//
// 	public static final class SendWarning implements WindowMapFunction<StockPrice, String> {
//
// 		private static final long serialVersionUID = 1L;
//
// 		@Override
// 		public void mapWindow(Iterable<StockPrice> values, Collector<String> out) throws Exception {
// 			if (values.iterator().hasNext()) {
// 				out.collect(values.iterator().next().symbol);
// 			}
// 		}
// 	}
//
// 	public static final class WindowCorrelation implements WindowMapFunction<Tuple2<Integer, Integer>, Double> {
//
// 		private static final long serialVersionUID = 1L;
// 		private Integer leftSum;
// 		private Integer rightSum;
// 		private Integer count;
//
// 		private Double leftMean;
// 		private Double rightMean;
//
// 		private Double cov;
// 		private Double leftSd;
// 		private Double rightSd;
//
// 		@Override
// 		public void mapWindow(Iterable<Tuple2<Integer, Integer>> values, Collector<Double> out) throws Exception {
//
// 			leftSum = 0;
// 			rightSum = 0;
// 			count = 0;
//
// 			cov = 0.;
// 			leftSd = 0.;
// 			rightSd = 0.;
//
// 			//compute mean for both sides, save count
// 			for (Tuple2<Integer, Integer> pair : values) {
// 				leftSum += pair.f0;
// 				rightSum += pair.f1;
// 				count++;
// 			}
//
// 			leftMean = leftSum.doubleValue() / count;
// 			rightMean = rightSum.doubleValue() / count;
//
// 			//compute covariance & std. deviations
// 			for (Tuple2<Integer, Integer> pair : values) {
// 				cov += (pair.f0 - leftMean) * (pair.f1 - rightMean) / count;
// 			}
//
// 			for (Tuple2<Integer, Integer> pair : values) {
// 				leftSd += Math.pow(pair.f0 - leftMean, 2) / count;
// 				rightSd += Math.pow(pair.f1 - rightMean, 2) / count;
// 			}
// 			leftSd = Math.sqrt(leftSd);
// 			rightSd = Math.sqrt(rightSd);
//
// 			out.collect(cov / (leftSd * rightSd));
// 		}
// 	}
//
// 	// *************************************************************************
// 	// UTIL METHODS
// 	// *************************************************************************
//
// 	private static boolean fileOutput = false;
// 	private static String hostName;
// 	private static int port;
// 	private static String outputPath;
//
// 	private static boolean parseParameters(String[] args) {
//
// 		// parse input arguments
// 		if (args.length == 3) {
// 			fileOutput = true;
// 			hostName = args[0];
// 			port = Integer.valueOf(args[1]);
// 			outputPath = args[2];
// 		} else if (args.length == 2) {
// 			hostName = args[0];
// 			port = Integer.valueOf(args[1]);
// 		} else {
// 			System.err.println("Usage: StockPrices <hostname> <port> [<output path>]");
// 			return false;
// 		}
// 		return true;
// 	}
//
// }
