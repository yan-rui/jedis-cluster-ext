# jedis-cluster-ext
Extention of jedis cluster, for batch operation. 

# usage

```java
// initial jediscluster
Set<HostAndPort> nodes = new HashSet<HostAndPort>();
nodes.add(new HostAndPort("127.0.0.1", 6379));
JedisCluster jedisCluster = new JedisCluster(nodes);

// initial pipeline
JedisClusterPipeline jcpipeline = new JedisClusterPipeline(jedisCluster);
jcpipeline.set("k1","v1");

// sync
jcpipeline.sync()

// close
jcpipeline.close()

```
