/*
 * Copyright (c) 2015 Huawei Technologies Co., Ltd. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.controller.cluster.common.actor;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Effect;
import akka.remote.ThisActorSystemQuarantinedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class listens to Akka RemotingLifecycleEvent events to detect when this node has been
 * quarantined by another. Once this node gets quarantined, restart the ActorSystem to allow this
 * node to rejoin the cluster.
 *
 * @author Gary Wu gary.wu1@huawei.com
 *
 */
// 一个actor的实现例子
public class QuarantinedMonitorActor extends UntypedActor {

    private static final Logger LOG = LoggerFactory.getLogger(QuarantinedMonitorActor.class);

    public static final String ADDRESS = "quarantined-monitor";

    private final Effect callback;
    private boolean quarantined;

    // 从blueprint看到传入此actor的props属性来自：QuarantinedMonitorActorPropsFactory
    // 在没细看再底层代码,猜测callback为QuarantinedMonitorActorPropsFactory中指定的函数: 其作用stop bundle
    protected QuarantinedMonitorActor(final Effect callback) {
        this.callback = callback;

        LOG.debug("Created QuarantinedMonitorActor");

        getContext().system().eventStream().subscribe(getSelf(), ThisActorSystemQuarantinedEvent.class);
    }

    @Override
    public void postStop() {
        LOG.debug("Stopping QuarantinedMonitorActor");
    }

    // actor接收message
    @Override
    public void onReceive(final Object message) throws Exception {
        final String messageType = message.getClass().getSimpleName();
        LOG.trace("onReceive {} {}", messageType, message);

        // check to see if we got quarantined by another node
        if (quarantined) {
            return;
        }

        // 被隔离
        if (message instanceof ThisActorSystemQuarantinedEvent) {
            final ThisActorSystemQuarantinedEvent event = (ThisActorSystemQuarantinedEvent) message;
            LOG.warn("Got quarantined by {}", event.remoteAddress());
            quarantined = true;

            // execute the callback
            // 应该是stop bundle动作
            callback.apply();
        }
    }

    public static Props props(final Effect callback) {
        return Props.create(QuarantinedMonitorActor.class, callback);
    }
}
