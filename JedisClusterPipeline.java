package yan.rui.jedis.cluster.ext;

import redis.clients.jedis.*;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.util.*;

/**
 * @Author: yan.rui
 * @Date: 2019/7/10
 * @Description: TODO
 **/
public class JedisClusterPipeline {
    private Map<String, JedisPool> nodeMap;
    private HashMap<String, Pipeline> pipelineHashMap = new HashMap<>();
    private HashMap<String, String> slotHostMap;
    private JedisCluster jedisCluster;

    public JedisClusterPipeline(JedisCluster jedisClusterP){
        startup(jedisClusterP);
    }

    public JedisClusterPipeline(){
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        // todo hosts
//        for (String redis_host : Config.redis_hosts) {
//            nodes.add(new HostAndPort(redis_host,6379));
//        }
        this.jedisCluster = new JedisCluster(nodes);
        startup(this.jedisCluster);
    }
    private HashMap<String, String> getSlotHostMap() {
        String anyHost = this.nodeMap.keySet().iterator().next();
        HashMap<String, String> slotmap = new HashMap<>();
        String parts[] = anyHost.split(":");
        HostAndPort anyHostAndPort = new HostAndPort(parts[0], Integer.parseInt(parts[1]));
        try{
            Jedis jedis = new Jedis(anyHostAndPort.getHost(), anyHostAndPort.getPort());
            List<Object> list = jedis.clusterSlots();
            for (Object object : list) {
                List<Object> list1 = (List<Object>) object;
                List<Object> master = (List<Object>) list1.get(2);
                String hostAndPort = new String((byte[]) master.get(0)) + ":" + master.get(1);
                slotmap.put(list1.get(0) +"-"+ list1.get(1),hostAndPort );
            }
            jedis.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return slotmap;
    }
    private void startup(JedisCluster jedisClusterP) {
        this.nodeMap = jedisClusterP.getClusterNodes();
        this.slotHostMap = getSlotHostMap();
        for( Map.Entry<String, JedisPool> en: this.nodeMap.entrySet( ) ){
            Jedis jedis = en.getValue().getResource();
            Pipeline pipeline = jedis.pipelined();
            String hostAndPort = en.getKey();
            this.pipelineHashMap.put(hostAndPort, pipeline);
        }
    }
    private String getHostAndPort(long slot) {
        String HostAndPortStr = "";
        for (Map.Entry<String, String> en : this.slotHostMap.entrySet()) {
            String[] slots = en.getKey().split("-");
            HostAndPortStr = en.getValue();
            if ((slot <= Long.valueOf(slots[0]) && slot >= Long.valueOf(slots[1]))
                    || (slot <= Long.valueOf(slots[1]) && slot >= Long.valueOf(slots[0]))) {
                return en.getValue();
            }
        }
        return HostAndPortStr;
    }

    public void set(String key, String value){
        Pipeline p = getPipelineByKey(key);
        p.set(key, value);
    }
    public void set(String key, String value,int expire){
        Pipeline p = getPipelineByKey(key);
        p.set(key, value);
        p.expire(key,expire);
    }
    public void hset(String key, String field, String value){
        Pipeline p = getPipelineByKey(key);
        p.hset(key, field, value);
    }
    public void hset(String key, String field, String value,int expire){
        Pipeline p = getPipelineByKey(key);
        p.hset(key, field, value);
        p.expire(key,expire);
    }

    private Pipeline getPipelineByKey(String key) {
        long slot = JedisClusterCRC16.getSlot(key);
        String HostAndPortStr = getHostAndPort(slot);
        return this.pipelineHashMap.get(HostAndPortStr);
    }

    public void sync() {
        for (Pipeline p: this.pipelineHashMap.values()){
            p.sync();
        }
    }

    public void close() throws IOException {
        this.jedisCluster.close();
    }

    public void closePipeline() throws IOException {
        for (Pipeline p: this.pipelineHashMap.values()){
            p.close();
        }
    }


    public static void main (String[] args){
        // 初始化jediscluster
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort("172.17.5.110", 6379));
        JedisCluster jedisCluster = new JedisCluster(nodes);

        // 初始化 pipeline
        JedisClusterPipeline jcpipeline = new JedisClusterPipeline(jedisCluster);

        jcpipeline.set("k1","v1");

    }
}
