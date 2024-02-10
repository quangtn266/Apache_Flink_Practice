package com.imooc.flink.wm;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EventTimeWaterMarkApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 5L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6000L);
        env.getConfig().setAutoWatermarkInterval(5000L);
        execute(env);
        env.execute("EventTimeWaterMarkApp");
    }

    public  static void execute(StreamExecutionEnvironment env) {
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-data") {};

        SingleOutputStreamOperator<String> window = env.socketTextStream("localhost", 9999)
                .filter(t -> !t.isEmpty())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {

                    @Override
                    public long extractTimestamp(String element) {
                        return Long.parseLong(element.split(",")[0]);
                    }
                }).map((MapFunction<String, Tuple2<String, Integer>>) value -> {
                    String[] splits = value.split(",");
                    return Tuple2.of(splits[1].trim(), Integer.parseInt(splits[2].trim()));
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag).reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> {
                    return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    final FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements,
                                        Collector<String> out) {
                        for(Tuple2<String, Integer> element: elements) {
                            out.collect("["+format.format(context.window().getStart())+"==>"+format.format(context.window().getEnd())
                            +"],"+element.f0+"==>"+element.f1);
                        }
                    }
                });

        List<HttpHost> httpHosts = Lists.newArrayList(new HttpHost("127.0.0.1",9200,"http"),
                new HttpHost("127.0.0.1", 9201, "http"));

        // Use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = getStringBuilder(httpHosts);

        // finally, build and add the sink to the job's pipeline
        window.addSink(esSinkBuilder.build()).name("esSink").setParallelism(4);

        window.print().setParallelism(1);
        DataStream<Tuple2<String, Integer>> sideOutput = window.getSideOutput(outputTag);
        sideOutput.print().setParallelism(1);
    }

    private static ElasticsearchSink.Builder<String> getStringBuilder(List<HttpHost> httpHosts) {
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);

                        return Requests.indexRequest().index("event_time_watermark_app")
                                .id(String.valueOf(UUID.randomUUID())).source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                });

        // Configuration for the bulk request, this instructs the sink to emit after every element
        // otherwise would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(restClientBuilder -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic",
                    "test123"));
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        });
        return esSinkBuilder;
    }
}
