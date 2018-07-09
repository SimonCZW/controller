/*
 * Copyright (c) 2015 Brocade Communications Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.controller.config.yang.config.actor_system_provider.impl;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import org.opendaylight.controller.cluster.ActorSystemProvider;
import org.opendaylight.controller.cluster.ActorSystemProviderListener;
import org.opendaylight.controller.cluster.common.actor.QuarantinedMonitorActor;
import org.opendaylight.controller.cluster.datastore.TerminationMonitor;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.util.ListenerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

// 提供akka actor system
public class ActorSystemProviderImpl implements ActorSystemProvider, AutoCloseable {
    private static final String ACTOR_SYSTEM_NAME = "opendaylight-cluster-data";
    static final Logger LOG = LoggerFactory.getLogger(ActorSystemProviderImpl.class);
    private final ActorSystem actorSystem;
    private final ListenerRegistry<ActorSystemProviderListener> listeners = new ListenerRegistry<>();

    public ActorSystemProviderImpl(
            // 个人理解Props是指定actor选项/配置
            //Props is a configuration class to specify options for the creation of actors,
            final ClassLoader classLoader, final Props quarantinedMonitorActorProps, final Config akkaConfig) {
        LOG.info("Creating new ActorSystem");

        // 创建akka actor system
        actorSystem = ActorSystem.create(ACTOR_SYSTEM_NAME, akkaConfig, classLoader);

        // 创建Termination monitor actor
        // 监听关闭actor system
        actorSystem.actorOf(Props.create(TerminationMonitor.class), TerminationMonitor.ADDRESS);

        // 创建Quarantined Monitor Actor
        // 用于监听Akka RemotingLifecycleEvent事件,如果节点被隔离，重启actor system去重新加入集群
        actorSystem.actorOf(quarantinedMonitorActorProps, QuarantinedMonitorActor.ADDRESS);
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public ListenerRegistration<ActorSystemProviderListener> registerActorSystemProviderListener(
            final ActorSystemProviderListener listener) {
        return listeners.register(listener);
    }

    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public void close() {
        LOG.info("Shutting down ActorSystem");

        try {
            Await.result(actorSystem.terminate(), Duration.create(10, TimeUnit.SECONDS));
        } catch (final Exception e) {
            LOG.warn("Error awaiting actor termination", e);
        }
    }
}
