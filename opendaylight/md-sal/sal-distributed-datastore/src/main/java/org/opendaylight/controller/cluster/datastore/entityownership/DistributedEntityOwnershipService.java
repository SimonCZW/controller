/*
 * Copyright (c) 2015 Brocade Communications Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.controller.cluster.datastore.entityownership;

import static org.opendaylight.controller.cluster.datastore.entityownership.EntityOwnersModel.CANDIDATE_NODE_ID;
import static org.opendaylight.controller.cluster.datastore.entityownership.EntityOwnersModel.ENTITY_OWNER_NODE_ID;
import static org.opendaylight.controller.cluster.datastore.entityownership.EntityOwnersModel.entityPath;

import akka.actor.ActorRef;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.opendaylight.controller.cluster.access.concepts.MemberName;
import org.opendaylight.controller.cluster.datastore.config.Configuration;
import org.opendaylight.controller.cluster.datastore.config.ModuleShardConfiguration;
import org.opendaylight.controller.cluster.datastore.entityownership.messages.RegisterCandidateLocal;
import org.opendaylight.controller.cluster.datastore.entityownership.messages.RegisterListenerLocal;
import org.opendaylight.controller.cluster.datastore.entityownership.messages.UnregisterCandidateLocal;
import org.opendaylight.controller.cluster.datastore.entityownership.messages.UnregisterListenerLocal;
import org.opendaylight.controller.cluster.datastore.entityownership.selectionstrategy.EntityOwnerSelectionStrategyConfig;
import org.opendaylight.controller.cluster.datastore.messages.CreateShard;
import org.opendaylight.controller.cluster.datastore.messages.GetShardDataTree;
import org.opendaylight.controller.cluster.datastore.shardstrategy.ModuleShardStrategy;
import org.opendaylight.controller.cluster.datastore.utils.ActorContext;
import org.opendaylight.mdsal.eos.common.api.CandidateAlreadyRegisteredException;
import org.opendaylight.mdsal.eos.common.api.EntityOwnershipState;
import org.opendaylight.mdsal.eos.dom.api.DOMEntity;
import org.opendaylight.mdsal.eos.dom.api.DOMEntityOwnershipCandidateRegistration;
import org.opendaylight.mdsal.eos.dom.api.DOMEntityOwnershipListener;
import org.opendaylight.mdsal.eos.dom.api.DOMEntityOwnershipListenerRegistration;
import org.opendaylight.mdsal.eos.dom.api.DOMEntityOwnershipService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.controller.md.sal.clustering.entity.owners.rev150804.EntityOwners;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.DataContainerChild;
import org.opendaylight.yangtools.yang.data.api.schema.MapEntryNode;
import org.opendaylight.yangtools.yang.data.api.schema.MapNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.tree.DataTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * The distributed implementation of the EntityOwnershipService.
 *
 * @author Thomas Pantelis
 */
public class DistributedEntityOwnershipService implements DOMEntityOwnershipService, AutoCloseable {
    @VisibleForTesting
    static final String ENTITY_OWNERSHIP_SHARD_NAME = "entity-ownership";

    private static final Logger LOG = LoggerFactory.getLogger(DistributedEntityOwnershipService.class);
    private static final Timeout MESSAGE_TIMEOUT = new Timeout(1, TimeUnit.MINUTES);

    private final ConcurrentMap<DOMEntity, DOMEntity> registeredEntities = new ConcurrentHashMap<>();
    private final ActorContext context;

    private volatile ActorRef localEntityOwnershipShard;
    private volatile DataTree localEntityOwnershipShardDataTree;

    DistributedEntityOwnershipService(final ActorContext context) {
        this.context = Preconditions.checkNotNull(context);
    }

    // invoke by blueprint: clustered-datastore.xml
    public static DistributedEntityOwnershipService start(final ActorContext context,
            final EntityOwnerSelectionStrategyConfig strategyConfig) {
        // context是什么: ActorContext 封装了shard manage actor在创建datastore时被创建的

        ActorRef shardManagerActor = context.getShardManager();

        Configuration configuration = context.getConfiguration();
        // 获取member列表?
        Collection<MemberName> entityOwnersMemberNames = configuration.getUniqueMemberNamesForAllShards();

        // 这里创建EntityOwnershipShard actor的build, 后面将此对象传递给manager去创建shard actor
        // Parameter1: module shard配置, the configuration of the new shard.
        // P2: used to obtain the Props for creating the shard actor instance.
        // P3: null, 意味着使用默认
        CreateShard createShard = new CreateShard(new ModuleShardConfiguration(EntityOwners.QNAME.getNamespace(),
                "entity-owners", ENTITY_OWNERSHIP_SHARD_NAME, ModuleShardStrategy.NAME, entityOwnersMemberNames),
                        newShardBuilder(context, strategyConfig), null);

        // 让shard manage actor创建local eos shard
        // 最终会调用ShardManager.onCreateShard()
        Future<Object> createFuture = context.executeOperationAsync(shardManagerActor,
                createShard, MESSAGE_TIMEOUT);

        createFuture.onComplete(new OnComplete<Object>() {
            @Override
            public void onComplete(final Throwable failure, final Object response) {
                if (failure != null) {
                    LOG.error("Failed to create {} shard", ENTITY_OWNERSHIP_SHARD_NAME, failure);
                } else {
                    LOG.info("Successfully created {} shard", ENTITY_OWNERSHIP_SHARD_NAME);
                }
            }
        }, context.getClientDispatcher());

        // 创建DistributedEOS
        return new DistributedEntityOwnershipService(context);
    }

    // 发消息给shardActor
    private void executeEntityOwnershipShardOperation(final ActorRef shardActor, final Object message) {
        Future<Object> future = context.executeOperationAsync(shardActor, message, MESSAGE_TIMEOUT);
        future.onComplete(new OnComplete<Object>() {
            @Override
            public void onComplete(final Throwable failure, final Object response) {
                if (failure != null) {
                    LOG.debug("Error sending message {} to {}", message, shardActor, failure);
                } else {
                    LOG.debug("{} message to {} succeeded", message, shardActor);
                }
            }
        }, context.getClientDispatcher());
    }

    @VisibleForTesting
    void executeLocalEntityOwnershipShardOperation(final Object message) {
        if (localEntityOwnershipShard == null) {
            // 找到entity-ownership的本地shard ActorRef/
            Future<ActorRef> future = context.findLocalShardAsync(ENTITY_OWNERSHIP_SHARD_NAME);
            future.onComplete(new OnComplete<ActorRef>() {
                @Override
                public void onComplete(final Throwable failure, final ActorRef shardActor) {
                    if (failure != null) {
                        LOG.error("Failed to find local {} shard", ENTITY_OWNERSHIP_SHARD_NAME, failure);
                    } else {
                        localEntityOwnershipShard = shardActor;
                        // 发消息给本地shard actor
                        executeEntityOwnershipShardOperation(localEntityOwnershipShard, message);
                    }
                }
            }, context.getClientDispatcher());

        } else {
            executeEntityOwnershipShardOperation(localEntityOwnershipShard, message);
        }
    }

    @Override
    public DOMEntityOwnershipCandidateRegistration registerCandidate(final DOMEntity entity)
            throws CandidateAlreadyRegisteredException {
        Preconditions.checkNotNull(entity, "entity cannot be null");

        // 判断entity是否在本地注册, 未注册则put
        if (registeredEntities.putIfAbsent(entity, entity) != null) {
            throw new CandidateAlreadyRegisteredException(entity);
        }

        // 封装entity的类，作为消息发送给 本地shard actor
        //  Message sent to the local EntityOwnershipShard to register a candidate.
        RegisterCandidateLocal registerCandidate = new RegisterCandidateLocal(entity);

        LOG.debug("Registering candidate with message: {}", registerCandidate);

        // 最终调用的是org.opendaylight.controller.cluster.datastore.entityownership.EntityOwnershipShard
        executeLocalEntityOwnershipShardOperation(registerCandidate);

        return new DistributedEntityOwnershipCandidateRegistration(entity, this);
    }

    // 取消注册candidate
    void unregisterCandidate(final DOMEntity entity) {
        LOG.debug("Unregistering candidate for {}", entity);

        executeLocalEntityOwnershipShardOperation(new UnregisterCandidateLocal(entity));
        registeredEntities.remove(entity);
    }

    // 注册entity类型的 listener
    @Override
    public DOMEntityOwnershipListenerRegistration registerListener(final String entityType,
            final DOMEntityOwnershipListener listener) {
        Preconditions.checkNotNull(entityType, "entityType cannot be null");
        Preconditions.checkNotNull(listener, "listener cannot be null");

        RegisterListenerLocal registerListener = new RegisterListenerLocal(listener, entityType);

        LOG.debug("Registering listener with message: {}", registerListener);

        // 发送消息给local shard actor
        executeLocalEntityOwnershipShardOperation(registerListener);
        return new DistributedEntityOwnershipListenerRegistration(listener, entityType, this);
    }

    // 获取entity的ownership
    @Override
    @SuppressFBWarnings(value = "NP_NULL_PARAM_DEREF", justification = "Unrecognised NullableDecl")
    public Optional<EntityOwnershipState> getOwnershipState(final DOMEntity forEntity) {
        Preconditions.checkNotNull(forEntity, "forEntity cannot be null");

        // 获取local eos shard data tree
        DataTree dataTree = getLocalEntityOwnershipShardDataTree();
        if (dataTree == null) {
            return Optional.absent();
        }

        // 从dataTree中读取传入entity
        java.util.Optional<NormalizedNode<?, ?>> entityNode = dataTree.takeSnapshot().readNode(
                entityPath(forEntity.getType(), forEntity.getIdentifier()));
        if (!entityNode.isPresent()) {
            return Optional.absent();
        }

        // Check if there are any candidates, if there are none we do not really have ownership state
        final MapEntryNode entity = (MapEntryNode) entityNode.get();
        final java.util.Optional<DataContainerChild<? extends PathArgument, ?>> optionalCandidates =
                entity.getChild(CANDIDATE_NODE_ID);
        final boolean hasCandidates = optionalCandidates.isPresent()
                && ((MapNode) optionalCandidates.get()).getValue().size() > 0;
        if (!hasCandidates) {
            return Optional.absent();
        }

        // 判断是否为当前阶段是 owner
        MemberName localMemberName = context.getCurrentMemberName();
        java.util.Optional<DataContainerChild<? extends PathArgument, ?>> ownerLeaf = entity.getChild(
            ENTITY_OWNER_NODE_ID);
        String owner = ownerLeaf.isPresent() ? ownerLeaf.get().getValue().toString() : null;
        boolean hasOwner = !Strings.isNullOrEmpty(owner);
        boolean isOwner = hasOwner && localMemberName.getName().equals(owner);

        return Optional.of(EntityOwnershipState.from(isOwner, hasOwner));
    }

    @Override
    public boolean isCandidateRegistered(@Nonnull final DOMEntity entity) {
        return registeredEntities.get(entity) != null;
    }

    // 获取local entity ownership shard的 data tree
    @VisibleForTesting
    @SuppressWarnings("checkstyle:IllegalCatch")
    DataTree getLocalEntityOwnershipShardDataTree() {
        if (localEntityOwnershipShardDataTree == null) {
            try {
                if (localEntityOwnershipShard == null) {
                    // 通过scale并发库Await, find local shard
                    localEntityOwnershipShard = Await.result(context.findLocalShardAsync(
                            ENTITY_OWNERSHIP_SHARD_NAME), Duration.Inf());
                }

                // 通过akka.pattern.Patterns ask请求 local entity ownership shard的dataTree
                // GetShardDataTree.INSTANCE 发送给actor的消息 获取shard的dataTree
                localEntityOwnershipShardDataTree = (DataTree) Await.result(Patterns.ask(localEntityOwnershipShard,
                        GetShardDataTree.INSTANCE, MESSAGE_TIMEOUT), Duration.Inf());
            } catch (Exception e) {
                LOG.error("Failed to find local {} shard", ENTITY_OWNERSHIP_SHARD_NAME, e);
            }
        }

        return localEntityOwnershipShardDataTree;
    }

    void unregisterListener(final String entityType, final DOMEntityOwnershipListener listener) {
        LOG.debug("Unregistering listener {} for entity type {}", listener, entityType);

        executeLocalEntityOwnershipShardOperation(new UnregisterListenerLocal(listener, entityType));
    }

    @Override
    public void close() {
    }

    /*
     * 这里创建 EntityOwnershipShard.Builder
     */
    private static EntityOwnershipShard.Builder newShardBuilder(final ActorContext context,
            final EntityOwnerSelectionStrategyConfig strategyConfig) {
        return EntityOwnershipShard.newBuilder().localMemberName(context.getCurrentMemberName())
                .ownerSelectionStrategyConfig(strategyConfig);
    }

    @VisibleForTesting
    ActorRef getLocalEntityOwnershipShard() {
        return localEntityOwnershipShard;
    }
}
