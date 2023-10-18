package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 工具
 * 方法1：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
 * 方法2：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓
 * <p>
 * 穿透/击穿问题
 * 方法3：根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存 穿透 问题
 * 方法4：根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存 击穿 问题
 */

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long ttl, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), ttl, unit);
    }

    public void setWithLogicExprie(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存穿透
    public <R, ID> R queryWithPassThrough(
            String prefixKey,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time,
            TimeUnit unit) {
        if (id == null) {
            return null;
        }
        // 1.从redis查询商铺缓存
        String key = prefixKey + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 2.1.存在直接返回
            return JSONUtil.toBean(json, type);
        }

        // 3.判断命中值是否为空值
        if (json != null) {
            return null;
        }

        // 4.缓存未命中，根据id查询数据库
        R res = dbFallback.apply(id);

        // 5.数据库中也不存在
        if (res == null) {
            // 6.解决缓存穿透，将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }

        // 7.存在，缓存商铺信息
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(res), time, unit);
        return res;
    }

    // 缓存击穿-逻辑过期
    public <R, ID> R queryWithLogicalExpire(
            String prefixKey,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time,
            TimeUnit unit) {

        String key = prefixKey + id;
        // 1. 从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 未命中，返回错误信息或返回空
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 3. 命中，判断当前数据是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        R res = JSONUtil.toBean((JSONObject) redisData.getData(), type);

        // 4. 未过期，返回商铺数据
        if (expireTime.isAfter(LocalDateTime.now())) {
            return res;
        }
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        // 5. 已过期，尝试获取互斥锁
        boolean isLock = tryLock(lockKey);

        // 7. 获得互斥锁，开启新线程
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    R newR = dbFallback.apply(id);
                    setWithLogicExprie(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 10. 释放互斥锁
                    unlock(lockKey);
                }
            });
        }

        // 6. 未获得互斥锁，返回旧商铺信息
        return res;
    }

    // 缓存击穿-互斥锁
    public <R, ID> R queryWithMutex(
            String prefixKey,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time,
            TimeUnit unit
    ) {
        if (id == null) {
            return null;
        }
        // 1.从redis查询商铺缓存
        String key = prefixKey + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 2.1.存在直接返回
            // Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return JSONUtil.toBean(json, type);
        }

        // 3.判断命中值是否为空值
        if (type != null) {
            return null;
        }

        // 4.实现缓存重构
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        R res;
        try {
            // 4.1 获取互斥锁
            boolean isLock = tryLock(lockKey);

            // 4.2 未获得互斥锁
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(prefixKey, id, type, dbFallback, time, unit);
            }

            // 4.3 获得互斥锁，根据id查询数据库
            res = dbFallback.apply(id);

            // 4.4 数据库中也不存在,返回错误信息
            if (res == null) {
                // 解决缓存穿透，将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }

            // 5. 存在，缓存商铺信息
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(res), time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6. 释放互斥锁
            unlock(lockKey);
        }

        return res;
    }

    // 设置互斥锁tryLock
    public Boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 删除互斥锁
    public void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
