package com.ericsson.genius

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

object RedisClient extends Serializable {
    val redisHost = "100.93.253.30"
    val redisPort = 16379
    val redisTimeout = 30000
    lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

    lazy val hook = new Thread {
        override def run = {
            println("Execute hook thread: " + this)
            pool.destroy()
        }
    }
    sys.addShutdownHook(hook.run)
}
