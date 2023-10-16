package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;
import static com.hmdp.utils.RedisConstants.LOGIN_CODE_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_CODE_TTL;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */

@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 不通过返回报错
            return Result.fail("手机号格式错误");
        }
        // 通过发送验证码
        String code = RandomUtil.randomNumbers(6);
        log.info("发送短信验证码成功，验证码：{}", code);

        // 保存验证码到session
//        session.setAttribute("code", code);

        // 保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 返回成功
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String userPhone = loginForm.getPhone();
        String userCode = loginForm.getCode();
        // 校验手机号
        if (RegexUtils.isPhoneInvalid(userPhone)) {
            // 不通过，返回错误
            return Result.fail("手机号格式不正确");
        }

        // 从session中取出验证码校验验证码
//        String code = (String) session.getAttribute("code");

        // 从redis中取出验证码校验验证码
        String code = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + userPhone);

        // 不通过，返回错误信息
        if (userCode == null || !userCode.equals(code)) {
            return Result.fail("验证码错误");
        }
        // 通过，用手机号查询用户
        User user = query().eq("phone", userPhone).one();
        // 查询到用户，返回部分非铭感用户信息
        if (user == null) {
            // 未查询到用户，使用手机号自动注册新用户，返回部分非铭感用户信息
            user = createUserWithPhone(userPhone);
        }

        // 保存用户到session
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));

        // 保存用户到redis
        String token = UUID.randomUUID().toString(true);
        String tokenKey = LOGIN_USER_KEY + token;
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

        // 将UserDTO转换为Map,putAll接受map格式
        Map<String, Object> stringObjectMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        stringRedisTemplate.opsForHash().putAll(tokenKey, stringObjectMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
