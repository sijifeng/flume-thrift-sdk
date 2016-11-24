package com.sijifeng.flume.javasdk;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.sijifeng.flume.javasdk.ThriftSend;
import com.sijifeng.flume.javasdk.thrift.Data;


public class TestThriftSend {
    
    @Test
    public void testThriftFlume(){
        String ip = "192.168.78.48";
        int port = 41414;
        
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("topic", "testflume");
        
        Data data = new Data();
        data.setEvent("test11");
        data.setHost("xyzs_httplog_server_tx_01");
        data.setTimestamp(1474979590);
        //data.setTimestamp(1474975590);

        Map<String, String> properties = new HashMap<>();
        properties.put("category","cqss");
        properties.put("cpu_average","86");
        properties.put("cpu1","100");
        properties.put("enumTest","muzhidao");

        data.setProperties(properties);
        ThriftSend send = new ThriftSend();
        send.append(data, headers, ip, port);
        
    }

}
