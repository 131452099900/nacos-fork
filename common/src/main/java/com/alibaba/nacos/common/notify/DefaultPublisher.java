/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * The default event publisher implementation.
 *
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class DefaultPublisher extends Thread implements EventPublisher {
    
    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    
    private volatile boolean initialized = false;
    
    private volatile boolean shutdown = false;

    // 事件类型
    private Class<? extends Event> eventType;

    // 订阅者
    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<>();

    private int queueMaxSize = -1;

    // 阻塞队列
    private BlockingQueue<Event> queue;
    
    protected volatile Long lastEventSequence = -1L;

    // CAS 最后一个事件序列
    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");

    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        // 实现了Thread的init作为守护线程
        setDaemon(true);
        setName("nacos.publisher-" + type.getName());
        this.eventType = type;
        // 队列的size
        this.queueMaxSize = bufferSize;
        if (this.queueMaxSize == -1) {
            this.queueMaxSize = ringBufferSize;
        }
        this.queue = new ArrayBlockingQueue<>(this.queueMaxSize);
        start();
    }
    
    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }
    
    @Override
    public synchronized void start() {
        if (!initialized) {
            // start just called once
            super.start();
            initialized = true;
        }
    }
    
    @Override
    public long currentEventSize() {
        return queue.size();
    }
    
    @Override
    public void run() {
        // 一直执行这个
        openEventHandler();
    }
    
    void openEventHandler() {
        try {
            
            // 消息积压问题 因为开始阻塞的话可能会导致大量的信息进入，如果真的很多就开始
            int waitTimes = 60;
            // To ensure that messages are not lost, enable EventHandler when
            // waiting for the first Subscriber to register
            // 为了确保消息不丢失，需要等待第一个事件来注册
            while (!shutdown && !hasSubscriber() && waitTimes > 0) {
                ThreadUtils.sleep(1000L);
                waitTimes--;
            }

            while (!shutdown) {
                // 阻塞获取事件
                final Event event = queue.take();
                receiveEvent(event);
                // 消费完事件更新序列号
                UPDATER.compareAndSet(this, lastEventSequence, Math.max(lastEventSequence, event.sequence()));
            }
        } catch (Throwable ex) {
            LOGGER.error("Event listener exception : ", ex);
        }
    }
    
    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber) {
        // 这里貌似用的也是同一个队列
        subscribers.add(subscriber);
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }
    
    @Override
    public boolean publish(Event event) {
        checkIsStart();
        // 阻塞放进去
        boolean success = this.queue.offer(event);

        // 如果没设置进去，就当场执行
        if (!success) {
            LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            receiveEvent(event);
            return true;
        }
        return true;
    }
    
    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Receive and notifySubscriber to process the event.
     *
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        // 事件序列号，相当于ID
        final long currentEventSequence = event.sequence();
        
        if (!hasSubscriber()) {
            LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
            return;
        }
        
        // Notification single event listener
        for (Subscriber subscriber : subscribers) {
            // 如果不匹配，返回
            if (!subscriber.scopeMatches(event)) {
                continue;
            }
            
            // 是否忽略过期事件，根据lastEventSequence和currentEventSequence进行管控 这里感觉根据策略来写会更好
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                LOGGER.debug("[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                        event.getClass());
                continue;
            }
            
            // Because unifying smartSubscriber and subscriber, so here need to think of compatibility.
            // Remove original judge part of codes.
            notifySubscriber(subscriber, event);
        }
    }
    
    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {
        
        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        
        final Runnable job = () -> subscriber.onEvent(event);
        final Executor executor = subscriber.executor();
        
        if (executor != null) {
            executor.execute(job);
        } else {
            try {
                job.run();
            } catch (Throwable e) {
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
