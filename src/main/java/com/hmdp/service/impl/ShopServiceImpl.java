package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;

import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import io.netty.util.internal.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
//        Shop shop = queryWithPassThrough(id);

        // 缓存击穿-互斥锁
//        Shop shop = queryWithMutex(id);

        // 缓存击穿-逻辑过期
        Shop shop = queryWithLogicExpire(id);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    // 缓存穿透
    public Shop queryWithPassThrough(Long id) {
        if (id == null) {
            return null;
        }
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 2.1.存在直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 3.判断命中值是否为空值
        if (shopJson != null) {
            return null;
        }

        // 4.缓存未命中，根据id查询数据库
        Shop shop = getById(id);

        // 5.数据库中也不存在
        if (shop == null) {
            // 6.解决缓存穿透，将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }

        // 7.存在，缓存商铺信息
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    // 缓存击穿-互斥锁
    public Shop queryWithMutex(Long id) {
        if (id == null) {
            return null;
        }
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 2.1.存在直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 3.判断命中值是否为空值
        if (shopJson != null) {
            return null;
        }

        // 4.实现缓存重构
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop;
        try {
            // 4.1 获取互斥锁
            boolean isLock = tryLock(lockKey);

            // 4.2 未获得互斥锁
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            // 4.3 获得互斥锁，根据id查询数据库
            shop = getById(id);

            // 4.4 数据库中也不存在,返回错误信息
            if (shop == null) {
                // 解决缓存穿透，将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }

            // 5. 存在，缓存商铺信息
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6. 释放互斥锁
            unlock(lockKey);
        }

        return shop;
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

    // 缓存击穿-逻辑过期
    private Shop queryWithLogicExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1. 从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 未命中，返回错误信息或返回空
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        // 3. 命中，判断当前数据是否过期
        RedisData redisShop = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisShop.getExpireTime();
        Shop shop = JSONUtil.toBean((JSONObject) redisShop.getData(), Shop.class);

        // 4. 未过期，返回商铺数据
        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        // 5. 已过期，尝试获取互斥锁
        boolean isLock = tryLock(lockKey);

        // 7. 获得互斥锁，开启新线程
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    this.saveShop2Redis(id, key);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 10. 释放互斥锁
                    unlock(lockKey);
                }
            });
        }


        // 6. 未获得互斥锁，返回旧商铺信息
        return shop;
    }

    // 缓存击穿-缓存重建
    private void saveShop2Redis(Long id, String key) {
        // 8. 根据ID查询数据库
        Shop res = getById(id);

        // 9. 封装缓存时间和商铺数据，写入Redis
        RedisData redisData = new RedisData();
        redisData.setData(res);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(20L));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺ID不能为空");
        }
        // 先操作数据库
        updateById(shop);
        String key = CACHE_SHOP_KEY + id;
        // 让缓存失效，缓存更新
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    @Override
    public Result queryShopByType() {
        return null;
    }
}
