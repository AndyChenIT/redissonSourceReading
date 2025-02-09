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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {
        
        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;
        
        public ExpirationEntry() {
            super();
        }
        
        public synchronized void addThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                counter = 1;
            } else {
                counter++;
            }
            threadIds.put(threadId, counter);
        }
        public synchronized boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public synchronized Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }
        public synchronized void removeThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                return;
            }
            counter--;
            if (counter == 0) {
                threadIds.remove(threadId);
            } else {
                threadIds.put(threadId, counter);
            }
        }
        
        
        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }
        
    }
    
    private static final Logger log = LoggerFactory.getLogger(RedissonLock.class);

    //这个map中的数据就代表持有锁的客户端，当lua脚本执行失败也就是没有锁的时候就从这个map去除
    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    final String id;
    final String entryName;

    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        //UUID
        this.id = commandExecutor.getConnectionManager().getId();
        //看门狗时间，30s
        //看门狗机制：监控持有锁的客户端（当前代码也正是加锁动作）是否存活着，
        //如果活着那么看门狗就不断延长锁过期时间，这样可以防止客户端崩溃了但是重启后导致另外客户端获取锁
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        // UUID + 'anyLock'
        this.entryName = id + ":" + name;
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    protected String getEntryName() {
        return entryName;
    }

    String getChannelName() {
        return prefixName("redisson_lock__channel", getName());
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    @Override
    public void lock() {
        try {
            //leaseTime默认-1，表示获取锁成功永久持有锁，否则过期时间默认是30s
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(-1, null, true);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        lock(leaseTime, unit, true);
    }

    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {
        long threadId = Thread.currentThread().getId();
        //剩余过期时间
        Long ttl = tryAcquire(-1, leaseTime, unit, threadId);
        // lock acquired
        // ttl 返回null表示加锁成功
        if (ttl == null) {
            return;
        }
        // 使用redis->subscribe订阅channel，用于监听回调处理
        RFuture<RedissonLockEntry> future = subscribe(threadId);
        if (interruptibly) {
            commandExecutor.syncSubscriptionInterrupted(future);
        } else {
            commandExecutor.syncSubscription(future);
        }

        // 获锁失败，则使用while循环不断获取锁的剩余过期时间ttl，然后指定Park的时间为ttl，不断循环判断
        try {
            while (true) {
                //再次执行
                ttl = tryAcquire(-1, leaseTime, unit, threadId);
                // lock acquired
                //获取锁成功，break退出死循环
                if (ttl == null) {
                    break;
                }

                // waiting for message
                if (ttl >= 0) {
                    try {
                        future.getNow().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (interruptibly) {
                            throw e;
                        }
                        future.getNow().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (interruptibly) {
                        future.getNow().getLatch().acquire();
                    } else {
                        future.getNow().getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally {
            unsubscribe(future, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }
    
    private Long tryAcquire(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync(waitTime, leaseTime, unit, threadId));
    }
    
    private RFuture<Boolean> tryAcquireOnceAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        if (leaseTime != -1) {
            return tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }
        RFuture<Boolean> ttlRemainingFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                                                                    TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            if (e != null) {
                return;
            }

            // lock acquired
            if (ttlRemaining) {
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }
    //  waitTime 表示尝试加锁失败时等待锁的时间，leaseTime表示加锁成功后等待 leaseTime 时间后释放锁。
    private <T> RFuture<Long> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        //当leaseTime为-1时才启用watchdog
        if (leaseTime != -1) {
            //不等于-1就是代表自定义了过期时间，不启用watchdog，会直接返回，可以看到并不会执行下面代码，
            // 也就是不会加看门狗机制，自定义的过期时间到了就直接超时掉
            return tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        }
        //当前锁剩余 过期时间
        //使用internalLockLeaseTime作为租期 默认30s过期
        RFuture<Long> ttlRemainingFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                                                                TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            //这里是null的原因在于lua脚本执行的时候成功返回 nil
            //这里代码是加锁失败的逻辑，此时 传参为 null, f.cause()
            //加锁失败也就是别的客户端持有锁
            if (e != null) {
                return;
            }

            //这里是加锁成功的逻辑，此时传参是 f.getNow(), null
            //也就是 ttlRemaining 是null，这里也可以看出来lua脚本执行的细节，执行成功返回的值其实是 null
            // lock acquired
            if (ttlRemaining == null) {
                //开启一个定时调度的任务，只要这个锁还被客户端持有着，那么就不会不断的去延长那个key的生存周期
                //客户端只要不手动释放锁，那么我就一直延长过期时间
                //所谓的延长过期时间就是 pexpire 命令更新过期时间为30s，然后延迟定时任务，只要一直持有锁，就一直延迟10s不断执行刷新更新时间
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    //延期操作
    private void renewExpiration() {
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }

                //Lua脚本延期锁的过期时间为30s
                RFuture<Boolean> future = renewExpirationAsync(threadId);
                future.onComplete((res, e) -> {
                    //客户端没有锁了，直接移除+返回
                    if (e != null) {
                        log.error("Can't update lock " + getName() + " expiration", e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }

                    //延期成功
                    if (res) {
                        // reschedule itself
                        // 继续循环延期操作
                        renewExpiration();
                    }
                });
            }
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);//成功加锁后，看门狗后续每隔10s去把过期时间重置为30s
        
        ee.setTimeout(task);
    }
    
    private void scheduleExpirationRenewal(long threadId) {
        ExpirationEntry entry = new ExpirationEntry();
        //map中的key为 this.entryName = id + ":" + name
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        //第2次以后再获取锁，不用再使用时间轮算法延期了
        if (oldEntry != null) {
            oldEntry.addThreadId(threadId);
        } else {
            //第1次获取成功时，进行延期操作
            entry.addThreadId(threadId);
            renewExpiration();
        }
    }

    /**
     * 更新过期时间
     */
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        //看是否 getName() 还存在这个key的map
        // pexpire 重新设置过期时间是 internalLockLeaseTime （30s）
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            task.removeThreadId(threadId);
        }

        if (threadId == null || task.hasNoThreads()) {
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        CommandBatchService executorService = createCommandBatchService();
        //执行lua脚本
        //选择一个slot来执行脚本
        RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
        if (commandExecutor instanceof CommandBatchService) {
            return result;
        }

        RPromise<T> r = new RedissonPromise<>();
        RFuture<BatchResult<?>> future = executorService.executeAsync();
        future.onComplete((res, ex) -> {
            if (ex != null) {
                r.tryFailure(ex);
                return;
            }

            r.trySuccess(result.getNow());
        });
        return r;
    }

    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        // Collections.singletonList(getName()) 就是你在 getLock("anyLock")中传入的参数，也是 KEY 列表
        // internalLockLeaseTime, getLockName(threadId) 这两个参数是 Object... params 对应 ARGV 列表
        // KEYS列表 ==> Collections.singletonList(getName()) 第一个参数
        // ARGV列表 ==> internalLockLeaseTime（ARGV[1]）, getLockName(threadId)（ARGV[2]）两个参数

        // (redis.call('exists', KEYS[1]) == 0) then  ==> 判断 anyLock 是否有这个key存在
        // redis.call('hincrby', KEYS[1], ARGV[2], 1) ==> anyLock 的一个map   之前是hset指令，后面是hincrby，这是可重入锁
        // anyLock :{
        //  “lockState”: 1,
        //  “otherKey”: “otherValue”
        //  }
        // redis.call('pexpire', KEYS[1], ARGV[1]) ==> 设置 anyLock 过期时间
        // 可以看出来是默认的-1时，这个key的过期时间就是 internalLockLeaseTime === 30s ARGV[1]
        // 不是-1就代表自定义了过期时间

        // (redis.call('hexists', KEYS[1], ARGV[2]) == 1  ===>存在这个key时，并且name也对的上，也就是当前线程再次拿锁---可重入锁
        // redis.call('hincrby', KEYS[1], ARGV[2], 1);  ===> 加1，表示可重入
        // redis.call('pexpire', KEYS[1], ARGV[1]); ===>把过期重新设置为 ARGV[1]==internalLockLeaseTime（默认30s）
        // redis.call('pttl', KEYS[1]) ===> 返回这个key的剩余过期时间

        //总结：默认lock方法代表锁默认30s过期，并且这个锁支持可重入，否则按照你传入的时间过期
        //当同一线程再次尝试拿锁时，此时过期时间累加 internalLockLeaseTime == 30s
        //如果加锁成功，返回nil，否则别的客户端持有锁，那么返回当前key的剩余过期时间
        //其实 map 中的 value（ARGV[2]）值就是 id+threadId == UUID + 当前线程id（比如 1 ）  这个也可以说明当前线程可重入
        return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                "if (redis.call('exists', KEYS[1]) == 0) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",
                Collections.singletonList(getName()), internalLockLeaseTime, getLockName(threadId));
    }

    private CommandBatchService createCommandBatchService() {
        if (commandExecutor instanceof CommandBatchService) {
            return (CommandBatchService) commandExecutor;
        }

        MasterSlaveEntry entry = commandExecutor.getConnectionManager().getEntry(getName());
        BatchOptions options = BatchOptions.defaults()
                                .syncSlaves(entry.getAvailableSlaves(), 1, TimeUnit.SECONDS);

        return new CommandBatchService(commandExecutor.getConnectionManager(), options);
    }

    private void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        get(acquireFailedAsync(waitTime, unit, threadId));
    }
    
    protected RFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    /**
     * @param waitTime 获取锁最大等待时间
     * @param leaseTime 获取锁后锁的超时时间
     */
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        //获取锁最大等待时间
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        Long ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
        // lock acquired
        if (ttl == null) {
            //ttl是null表示获取成功
            return true;
        }

        //剩余最多等待时间（相当于获取锁耗费了多长时间）
        time -= System.currentTimeMillis() - current;
        if (time <= 0) {
            //标记获取失败
            acquireFailed(waitTime, unit, threadId);
            return false;
        }
        
        current = System.currentTimeMillis();
        RFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        if (!subscribeFuture.await(time, TimeUnit.MILLISECONDS)) {
            if (!subscribeFuture.cancel(false)) {
                subscribeFuture.onComplete((res, e) -> {
                    if (e == null) {
                        unsubscribe(subscribeFuture, threadId);
                    }
                });
            }
            acquireFailed(waitTime, unit, threadId);
            return false;
        }

        try {
            //再次减去上面的步骤耗时
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                acquireFailed(waitTime, unit, threadId);
                return false;
            }

            //倘若一直获取失败，那么一直循环获取并且减去耗时，知道获取成功或者超时
            while (true) {
                long currentTime = System.currentTimeMillis();
                ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
                // lock acquired
                if (ttl == null) {
                    return true;
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }

                // waiting for message
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    subscribeFuture.getNow().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    subscribeFuture.getNow().getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }
            }
        } finally {
            unsubscribe(subscribeFuture, threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        pubSub.unsubscribe(future.getNow(), getEntryName(), getChannelName());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    public void unlock() {
        try {
            //里面 unlockAsync 方法是异步化的 get()后则是同步，相当于异步转同步
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }
        
//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                        + "redis.call('publish', KEYS[2], ARGV[1]); "
                        + "return 1 "
                        + "else "
                        + "return 0 "
                        + "end",
                Arrays.asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.writeAsync(getName(), codec, RedisCommands.EXISTS, getName());
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", ValueType.MAP_VALUE, new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, HGET, getName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        //脚本代码逻辑：

        //看当前 是否有 'anyLock' 的 map 并且有key 为 UUID + threadId
        //不存在返回nil

        //存在，表示当前有客户端持有锁
        //对重入次数减一，然后算一下还有多少重入次数

        //倘若还有重入次数，刷新过期时间为 internalLockLeaseTime （30s）
        //返回0

        //没有重入次数，直接删除 anyLock
        //发布 UNLOCK_MESSAGE 订阅消息
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                        "return nil;" +
                        "end; " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                        "return 0; " +
                        "else " +
                        "redis.call('del', KEYS[1]); " +
                        "redis.call('publish', KEYS[2], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return nil;",
                Arrays.asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        //执行lua脚本
        RFuture<Boolean> future = unlockInnerAsync(threadId);

        future.onComplete((opStatus, e) -> {
            cancelExpirationRenewal(threadId);

            if (e != null) {
                result.tryFailure(e);
                return;
            }

            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                result.tryFailure(cause);
                return;
            }

            result.trySuccess(null);
        });

        return result;
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        RFuture<Long> ttlFuture = tryAcquireAsync(-1, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((res, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
        });

        return result;
    }

    private void lockAsync(long leaseTime, TimeUnit unit,
            RFuture<RedissonLockEntry> subscribeFuture, RPromise<Void> result, long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync(-1, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(subscribeFuture, currentThreadId);
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(subscribeFuture, currentThreadId);
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            RedissonLockEntry entry = subscribeFuture.getNow();
            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            } else {
                // waiting for message
                AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                Runnable listener = () -> {
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                };

                entry.addListener(listener);

                if (ttl >= 0) {
                    Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            if (entry.removeListener(listener)) {
                                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                            }
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryAcquireOnceAsync(-1, -1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit,
            long currentThreadId) {
        RPromise<Boolean> result = new RedissonPromise<Boolean>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);
            
            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }
            
            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((r, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);
                
                tryLockAsync(time, waitTime, leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        if (!subscribeFuture.isDone()) {
                            subscribeFuture.cancel(false);
                            trySuccessFalse(currentThreadId, result);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return result;
    }

    private void trySuccessFalse(long currentThreadId, RPromise<Boolean> result) {
        acquireFailedAsync(-1, null, currentThreadId).onComplete((res, e) -> {
            if (e == null) {
                result.trySuccess(false);
            } else {
                result.tryFailure(e);
            }
        });
    }

    private void tryLockAsync(AtomicLong time, long waitTime, long leaseTime, TimeUnit unit,
            RFuture<RedissonLockEntry> subscribeFuture, RPromise<Boolean> result, long currentThreadId) {
        if (result.isDone()) {
            unsubscribe(subscribeFuture, currentThreadId);
            return;
        }
        
        if (time.get() <= 0) {
            unsubscribe(subscribeFuture, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }
        
        long curr = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
                if (e != null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(e);
                    return;
                }

                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }
                
                long el = System.currentTimeMillis() - curr;
                time.addAndGet(-el);
                
                if (time.get() <= 0) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                // waiting for message
                long current = System.currentTimeMillis();
                RedissonLockEntry entry = subscribeFuture.getNow();
                if (entry.getLatch().tryAcquire()) {
                    tryLockAsync(time, waitTime, leaseTime, unit, subscribeFuture, result, currentThreadId);
                } else {
                    AtomicBoolean executed = new AtomicBoolean();
                    AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                    Runnable listener = () -> {
                        executed.set(true);
                        if (futureRef.get() != null) {
                            futureRef.get().cancel();
                        }

                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);
                        
                        tryLockAsync(time, waitTime, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    };
                    entry.addListener(listener);

                    long t = time.get();
                    if (ttl >= 0 && ttl < time.get()) {
                        t = ttl;
                    }
                    if (!executed.get()) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                if (entry.removeListener(listener)) {
                                    long elapsed = System.currentTimeMillis() - current;
                                    time.addAndGet(-elapsed);
                                    
                                    tryLockAsync(time, waitTime, leaseTime, unit, subscribeFuture, result, currentThreadId);
                                }
                            }
                        }, t, TimeUnit.MILLISECONDS);
                        futureRef.set(scheduledFuture);
                    }
                }
        });
    }


}
