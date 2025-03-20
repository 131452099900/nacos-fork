/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.api.grpc.auto.RequestGrpc;
import com.alibaba.nacos.api.remote.RpcScheduledExecutor;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.request.ServerCheckRequest;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.api.remote.response.ServerCheckResponse;
import com.alibaba.nacos.common.remote.client.grpc.GrpcUtils;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.core.remote.Connection;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.core.remote.RequestHandlerRegistry;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * rpc request acceptor of grpc.
 * 实现类
 * @author liuzunfei
 * @version $Id: GrpcCommonRequestAcceptor.java, v 0.1 2020年09月01日 10:52 AM liuzunfei Exp $
 */
@Service
public class GrpcRequestAcceptor extends RequestGrpc.RequestImplBase {
    
    @Autowired
    RequestHandlerRegistry requestHandlerRegistry;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    private void traceIfNecessary(Payload grpcRequest, boolean receive) {
        String clientIp = grpcRequest.getMetadata().getClientIp();
        String connectionId = GrpcServerConstants.CONTEXT_KEY_CONN_ID.get();
        try {
            if (connectionManager.traced(clientIp)) {
                Loggers.REMOTE_DIGEST.info("[{}]Payload {},meta={},body={}", connectionId, receive ? "receive" : "send",
                        grpcRequest.getMetadata().toByteString().toStringUtf8(),
                        grpcRequest.getBody().toByteString().toStringUtf8());
            }
        } catch (Throwable throwable) {
            Loggers.REMOTE_DIGEST.error("[{}]Monitor request error,payload={},error={}", connectionId, clientIp,
                    grpcRequest.toByteString().toStringUtf8());
        }
        
    }

    /**
     * Payload会
     * @param grpcRequest 接收一个请求
     * @param responseObserver responseObserver监听者
     */
    @Override
    public void request(Payload grpcRequest, StreamObserver<Payload> responseObserver) {
        // 判断connection的IP是否合法
        traceIfNecessary(grpcRequest, true);
        String type = grpcRequest.getMetadata().getType();
        long startTime = System.nanoTime();
        
        //
        if (!ApplicationUtils.isStarted()) {
            Payload payloadResponse = GrpcUtils.convert(
                    ErrorResponse.build(NacosException.INVALID_SERVER_STATUS, "Server is starting,please try later."));
            traceIfNecessary(payloadResponse, false);

            // 接收到信息 每次客户端写入一个 Point 到消息流时，拿到特性和其它信息。
            responseObserver.onNext(payloadResponse);
            // 结束服务器端的调用
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    NacosException.INVALID_SERVER_STATUS, null, null, System.nanoTime() - startTime);
            return;
        }

        // 服务端检查 直接返回本连接ID就可以
        if (ServerCheckRequest.class.getSimpleName().equals(type)) {
            // 对于服务来检查 直接返回本连接ID就可以
            Payload serverCheckResponseP = GrpcUtils.convert(new ServerCheckResponse(GrpcServerConstants.CONTEXT_KEY_CONN_ID.get(), true));
            traceIfNecessary(serverCheckResponseP, false);
            responseObserver.onNext(serverCheckResponseP);
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(type, true,
                    0, null, null, System.nanoTime() - startTime);
            return;
        }

        // 从type中获取到RequestHandler
        RequestHandler requestHandler = requestHandlerRegistry.getByRequestType(type);
        // 如果没有找到对应的RequestHandler
        if (requestHandler == null) {
            Loggers.REMOTE_DIGEST.warn(String.format("[%s] No handler for request type : %s :", "grpc", type));
            // 返回一个错误响应
            Payload payloadResponse = GrpcUtils
                    .convert(ErrorResponse.build(NacosException.NO_HANDLER, "RequestHandler Not Found"));
            traceIfNecessary(payloadResponse, false);
            responseObserver.onNext(payloadResponse);
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    NacosException.NO_HANDLER, null, null, System.nanoTime() - startTime);
            return;
        }
        
        // 检查连接状态
        String connectionId = GrpcServerConstants.CONTEXT_KEY_CONN_ID.get();
        // 连接管理器中查看是否存在该连接，
        boolean requestValid = connectionManager.checkValid(connectionId);
        if (!requestValid) {
            Loggers.REMOTE_DIGEST
                    .warn("[{}] Invalid connection Id ,connection [{}] is un registered ,", "grpc", connectionId);
            // 返回Connection is unregistered.
            Payload payloadResponse = GrpcUtils
                    .convert(ErrorResponse.build(NacosException.UN_REGISTER, "Connection is unregistered."));
            traceIfNecessary(payloadResponse, false);
            responseObserver.onNext(payloadResponse);
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    NacosException.UN_REGISTER, null, null, System.nanoTime() - startTime);
            return;
        }
        
        Object parseObj = null;
        try {
            // 反序列化处理 payLoad -> GrpcRequest
            parseObj = GrpcUtils.parse(grpcRequest);
        } catch (Exception e) {
            Loggers.REMOTE_DIGEST
                    .warn("[{}] Invalid request receive from connection [{}] ,error={}", "grpc", connectionId, e);
            Payload payloadResponse = GrpcUtils.convert(ErrorResponse.build(NacosException.BAD_GATEWAY, e.getMessage()));
            traceIfNecessary(payloadResponse, false);
            responseObserver.onNext(payloadResponse);
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    NacosException.BAD_GATEWAY, e.getClass().getSimpleName(), null, System.nanoTime() - startTime);
            return;
        }

        // 如果为null，说明反序列化失败
        if (parseObj == null) {
            Loggers.REMOTE_DIGEST.warn("[{}] Invalid request receive  ,parse request is null", connectionId);
            // 返回request请求失败
            Payload payloadResponse = GrpcUtils
                    .convert(ErrorResponse.build(NacosException.BAD_GATEWAY, "Invalid request"));
            traceIfNecessary(payloadResponse, false);
            responseObserver.onNext(payloadResponse);
            responseObserver.onCompleted();

            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    NacosException.BAD_GATEWAY, null, null, System.nanoTime() - startTime);
            return;
        }
        
        if (!(parseObj instanceof Request)) {
            Loggers.REMOTE_DIGEST
                    .warn("[{}] Invalid request receive  ,parsed payload is not a request,parseObj={}", connectionId,
                            parseObj);
            // 进一步校验确认request
            Payload payloadResponse = GrpcUtils
                    .convert(ErrorResponse.build(NacosException.BAD_GATEWAY, "Invalid request"));
            traceIfNecessary(payloadResponse, false);
            responseObserver.onNext(payloadResponse);
            responseObserver.onCompleted();

            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    NacosException.BAD_GATEWAY, null, null, System.nanoTime() - startTime);
            return;
        }

        // 解析成headers 和 requestId
        Request request = (Request) parseObj;
        try {
            // 从连接管理器获取到Connection
            Connection connection = connectionManager.getConnection(GrpcServerConstants.CONTEXT_KEY_CONN_ID.get());
            // ip connectionId version 标签
            RequestMeta requestMeta = new RequestMeta();
            requestMeta.setClientIp(connection.getMetaInfo().getClientIp());
            requestMeta.setConnectionId(GrpcServerConstants.CONTEXT_KEY_CONN_ID.get());
            requestMeta.setClientVersion(connection.getMetaInfo().getVersion());
            // 类似于对象头?
            requestMeta.setLabels(connection.getMetaInfo().getLabels());
            requestMeta.setAbilityTable(connection.getAbilityTable());

            // 刷新连接活跃时间
            connectionManager.refreshActiveTime(requestMeta.getConnectionId());

            // 由对应的handler处理
            Response response = requestHandler.handleRequest(request, requestMeta);
            Payload payloadResponse = GrpcUtils.convert(response);
            traceIfNecessary(payloadResponse, false);
            if (response.getErrorCode() == NacosException.OVER_THRESHOLD) {
                RpcScheduledExecutor.CONTROL_SCHEDULER.schedule(() -> {
                    traceIfNecessary(payloadResponse, false);
                    responseObserver.onNext(payloadResponse);
                    responseObserver.onCompleted();
                }, 1000L, TimeUnit.MILLISECONDS);
            } else {
                traceIfNecessary(payloadResponse, false);
                responseObserver.onNext(payloadResponse);
                responseObserver.onCompleted();
            }
            MetricsMonitor.recordGrpcRequestEvent(type, response.isSuccess(),
                    response.getErrorCode(), null, request.getModule(), System.nanoTime() - startTime);
        } catch (Throwable e) {
            Loggers.REMOTE_DIGEST
                    .error("[{}] Fail to handle request from connection [{}] ,error message :{}", "grpc", connectionId,
                            e);
            Payload payloadResponse = GrpcUtils.convert(ErrorResponse.build(e));
            traceIfNecessary(payloadResponse, false);
            responseObserver.onNext(payloadResponse);
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(type, false,
                    ResponseCode.FAIL.getCode(), e.getClass().getSimpleName(), request.getModule(), System.nanoTime() - startTime);
        }
        
    }
    
}