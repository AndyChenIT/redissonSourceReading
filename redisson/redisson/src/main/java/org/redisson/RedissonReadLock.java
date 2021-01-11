/**
 * Copyright (c) 2013-2020 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonReadLock extends RedissonLock implements RLock {

    public RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getName(), getLockName(threadId)) + ":rwlock_timeout"; 
    }

    /**
     * 加读锁
     */
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        // anyLock: {
        //  “mode”: “xxxx”
        // }

        //之前是读锁，再次成功加锁后成这样
        //anyLock: {
        //  “mode”: “read”,
        //  “UUID_01:threadId_01”: 1,
        //  “UUID_02:threadId_02”: 1
        //}
        //{anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
        //{anyLock}:UUID_02:threadId_02:rwlock_timeout:1		1
        return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                "if (mode == false) then " +
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +                                                 //hset anyLock mode read  加读锁
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +                                                     //hset anyLock UUID_01:threadId_01 1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +                                                       //set {anyLock}:UUID_01:threadId_01:rwlock_timeout:1  1
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +                                             //pexpire {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 30000
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +                                                     //pexpire anyLock 30000
                                  "return nil; " +
                                "end; " +
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " + //有人加了读锁或者自己加了写锁
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +                                      //hincrby anyLock UUID_02:threadId_02 1   ind就是累加后的值  可重入锁
                                  "local key = KEYS[2] .. ':' .. ind;" +                                                            //{anyLock}:UUID_02:threadId_02:rwlock_timeout:1
                                  "redis.call('set', key, 1); " +                                                                   //set {anyLock}:UUID_01:threadId_01:rwlock_timeout:1  1    set {anyLock}:UUID_01:threadId_01:rwlock_timeout:2（可重入）  1
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",                                                               //返回剩余超时时间
                        Arrays.<Object>asList(getName(), getReadWriteTimeoutNamePrefix(threadId)), //KEYS: anyLock , {anyLock}:UUID_01:threadId_01:rwlock_timeout
                        internalLockLeaseTime, getLockName(threadId), getWriteLockName(threadId)); //ARGV: 30s , UUID_01:threadId_01 , UUID_01:threadId_01:write
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                    
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " + 
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " + 
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " + 
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +
                            
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 
                        
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; ",
                Arrays.<Object>asList(getName(), getChannelName(), timeoutPrefix, keyPrefix), 
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }

    /**
     * 看门狗每隔10s进行刷新过期时间
     */
    @Override
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        //anyLock: {
        //  “mode”: “read”,
        //  “UUID_01:threadId_01”: 1
        //}
        //这里刷新了两个时间
        //pexpire anyLock 30000
        //pexpire {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 30000

        //下面有两个for循环，第一个循环是针对key的，第二个循环是每个锁上的可重入锁
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +                                           //hget anyLock UUID_01:threadId_01  返回1，
                "if (counter ~= false) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +                                                           //pexpire anyLock 30000 ，刷新anyLock锁生存时间为30s
                    
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +                                                          //hlen anyLock = 2 > 1  读锁超过1个
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " +                                      //拿到每个key的值
                            "if type(counter) == 'number' then " +                                                          //是数字，比如上文中 UUID_01:threadId_01
                                "for i=counter, 1, -1 do " + 
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " +  //pexpire {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 30000 这里就是对可重入锁的每个锁都刷新
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getName(), keyPrefix),  //KEYS: anyLock , {anyLock}
            internalLockLeaseTime, getLockName(threadId));//ARGV: 30s , UUID_01:threadId_01
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.HGET, getName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
