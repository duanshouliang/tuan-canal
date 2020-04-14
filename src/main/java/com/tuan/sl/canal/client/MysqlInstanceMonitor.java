package com.tuan.sl.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.tuan.sl.canal.executor.DataPipelineExecutor;
import com.tuan.sl.canal.executor.PipelineExecutor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

@Component
public class MysqlInstanceMonitor implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlInstanceMonitor.class);
    private volatile boolean refresh = false;
    private CountDownLatch countDownLatch;
    private ZooKeeper zooKeeper;
    private List<PipelineExecutor> pipelineExecutors;
    private CanalContext canalContext;

    public MysqlInstanceMonitor(){
        countDownLatch = new CountDownLatch(1);
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:2181",3000, this);
            countDownLatch.await();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.None || watchedEvent.getState() == SyncConnected){
            countDownLatch.countDown();
        }
    }

    private void buildCanalConnection(){
        List<String> instances = null;
        //从zookeeper中获取mysql instance原数据
        try {
            instances = zooKeeper.getChildren("/otter/canal/destinations", watchedEvent -> {
                //zookeeper子节点有变更时，则将refresh设置为true（重新加载instance）
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("Refresh instance");
                    refresh = true;
                }
            });
        } catch (KeeperException e) {
            e.printStackTrace();
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }
        if(null == instances || instances.size() == 0){
            return;
        }
        disconnect();
        instances.forEach(instance ->{
            CanalConnector connector = CanalConnectors.newClusterConnector("127.0.0.1:2181", instance, "", "");
            connector.connect();
            connector.subscribe();
            connector.rollback();

        });
    }

    private void disconnect(){
        LOGGER.info("Disconnect from old connections");
        if(null == pipelineExecutors || pipelineExecutors.size() == 0)
            return;
        CountDownLatch countDownLatch = new CountDownLatch(pipelineExecutors.size());
        pipelineExecutors.forEach(pipelineExecutor ->{
            pipelineExecutor.stop(countDownLatch);
        });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Disconnect from old connection with exception {}", e.getMessage());
        }
        pipelineExecutors.clear();
        LOGGER.info("All connection has disconnected!");
    }

    public static void main(String[] args) {
        MysqlInstanceMonitor watcher = new MysqlInstanceMonitor();
    }
}
