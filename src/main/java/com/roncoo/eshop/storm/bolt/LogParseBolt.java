package com.roncoo.eshop.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志解析的bolt
 */
public class LogParseBolt extends BaseRichBolt {

    private static final long serialVersionUID = -8017609899644290359L;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogParseBolt.class);

    private OutputCollector collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        /**
         * {
         *"product_module": "product_detail_info",
         *"method": "GET",
         *"http_version": 1.1,
         *"raw_reader": "GET /product?productId=7&shopId=7 HTTP/1.1\r\nHost: 192.168.8.81\r\nUser-Agent: lua-resty-http/0.11 (Lua) ngx_lua/9014\r\n\r\n",
         *"uri_args": {
         *"productId": "7",
         *"shopId": "7"
         *},
         *"headers": {
         *"host": "192.168.8.81",
         *"user-agent": "lua-resty-http/0.11 (Lua) ngx_lua/9014"
         *}
         *}
         */

        String message = tuple.getStringByField("message");
        LOGGER.info("【LogParseBolt接收到一条日志】message=" + message);
        JSONObject messageJSON = JSONObject.parseObject(message);
        JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args");
        Long productId = uriArgsJSON.getLong("productId");

        if (productId != null) {
            collector.emit(new Values(productId));
            LOGGER.info("【LogParseBolt发射出去一个商品id】productId=" + productId);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId"));
    }

}
