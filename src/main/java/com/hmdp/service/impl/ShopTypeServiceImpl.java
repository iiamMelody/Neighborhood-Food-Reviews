package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_LIST;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ShopTypeMapper shopTypeMapper;
    @Override
    public Result queryTypeList() {
        //1. 查询redis是否存在
        String shopType = stringRedisTemplate.opsForValue().get(CACHE_SHOP_LIST);

        //2. redis存在，直接返回
        if (StrUtil.isNotBlank(shopType)) {
            List<ShopType> shopTypeList = JSONUtil.toList(shopType, ShopType.class);
            return Result.ok(shopTypeList);
        }

        //3. redis不存在，查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //4.数据库不存在
        if (shopTypes == null) {
            return Result.fail("分类不存在");
        }

        //5. 数据库存在，写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_LIST,JSONUtil.toJsonStr(shopTypes));

        //5.返回
        return Result.ok(shopTypes);
    }
}
