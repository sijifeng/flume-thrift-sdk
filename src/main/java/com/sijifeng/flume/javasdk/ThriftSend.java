package com.sijifeng.flume.javasdk;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sijifeng.flume.javasdk.thrift.Data;
import com.sijifeng.flume.javasdk.thrift.ThriftFlumeEvent;
import com.sijifeng.flume.javasdk.thrift.ThriftSourceProtocol;

public class ThriftSend {
    private static final Logger logger = LoggerFactory.getLogger(ThriftSend.class);
    
    public static final int TIMEOUT = 30000;
    static TTransport transport = null;
    static ThriftSourceProtocol.Client client = null;
    
    private synchronized static void init(String thriftIp, int port) throws TTransportException{
        transport = new TFastFramedTransport(new TSocket(thriftIp, port, TIMEOUT));
        // 协议要和服务端一致
        TProtocol protocol = new TCompactProtocol(transport);
        client = new ThriftSourceProtocol.Client(protocol);   
        transport.open();
    }
    
    public static boolean append(Data data, Map<String, String> headers, String thriftIp, int port){
        boolean flag = false;
        try {
            init(thriftIp, port);
            ThriftFlumeEvent event = new ThriftFlumeEvent(headers, ByteBuffer.wrap(data.toJson().getBytes()));
            System.out.println(event.toString());
            client.append(event);
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
            //logger.error(e.getMessage());
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return flag;
    }
    
    
    public static boolean appendBatch(List<Data> datas, Map<String, String> headers, String thriftIp, int port){
        boolean flag = false;
        try {
            init(thriftIp, port);
            List<ThriftFlumeEvent> events = new LinkedList<ThriftFlumeEvent>();
            for(Data data : datas){
                if(null != data){
                    ThriftFlumeEvent event = new ThriftFlumeEvent(headers, ByteBuffer.wrap(data.toJson().getBytes()));
                    events.add(event);
                }
            }
            client.appendBatch(events);
            flag = true;
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return flag;
    }
    
}
