# jedis-cluster-ext
Extention of jedis cluster, for batch pipeline. 

# usage

```java
        // 初始化jediscluster
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort("127.0.0.1", 6379));
        JedisCluster jedisCluster = new JedisCluster(nodes);

        // 初始化 pipeline
        JedisClusterPipeline jcpipeline = new JedisClusterPipeline(jedisCluster);

        jcpipeline.set("k1","v1");
        
        

```
