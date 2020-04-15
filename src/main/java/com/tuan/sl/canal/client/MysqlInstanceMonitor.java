package com.tuan.sl.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.tuan.sl.canal.executor.PipelineExecutor;
import com.tuan.sl.kafka.KafkaProducerWrapper;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

@Component
public class MysqlInstanceMonitor implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlInstanceMonitor.class);

    private static final String DEFAULT_ZOOKEEPER_SERVER="127.0.0.1:2181";
    private volatile boolean refresh = false;
    private CountDownLatch countDownLatch;
    private ZooKeeper zooKeeper;
    //数据同步执行器池
    private List<PipelineExecutor> pipelineExecutors;
    private CanalContext canalContext;
    private KafkaProducerWrapper kafkaProducer;
    //数据同步线程执行器
    private ExecutorService executors;
    private Thread startThread;

    public MysqlInstanceMonitor(KafkaProducerWrapper kafkaProducer, CanalContext canalContext){
        this.kafkaProducer = kafkaProducer;
        this.canalContext = canalContext;
        countDownLatch = new CountDownLatch(1);
        pipelineExecutors = new ArrayList<>();
        //创建线程池
        ThreadFactory threadFactory = new DefaultThreadFactory("Instance executor") {
            @Override
            protected Thread newThread(Runnable r, String name) {
                return new Thread(r, name);
            }
        };
        //创建线程执行器
        executors = Executors.newCachedThreadPool(threadFactory);
        try {
            zooKeeper = new ZooKeeper(DEFAULT_ZOOKEEPER_SERVER,3000, this);
            //最初与zk服务器建立好连接或者连接成功时，则执行后事件
            countDownLatch.await();
        } catch (IOException e) {
            LOGGER.error("Connect to zookeeper with exception {}", e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error("Connect to zookeeper with exception {}", e.getMessage());
        }
        startThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!Thread.interrupted()){
                    if(refresh){
                        buildCanalInstance();
                        refresh = false;
                    }
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        startThread.start();
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        //最初与zk服务器建立好连接或者连接成功时，则执行后事件
        if(watchedEvent.getType() == Event.EventType.None || watchedEvent.getState() == SyncConnected){
            countDownLatch.countDown();
        }
    }

    public void init(){
        this.buildCanalInstance();
    }

    private void buildCanalInstance(){
        List<String> instances = null;
        //从zookeeper中获取canal instance原数据
        try {
            instances = zooKeeper.getChildren("/otter/canal/destinations", watchedEvent -> {
                //异步监听zookeeper子节点是否有变更，若有变更则将refresh设置为true（重新加载instance）
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("Refresh instance");
                    refresh = true;
                }
            });
        } catch (KeeperException e) {
            LOGGER.error("Get canal instance with exception {}", e.getMessage());
            return;
        } catch (InterruptedException e) {
            LOGGER.error("Get canal instance with exception {}", e.getMessage());
            return;
        }
        disconnect();
        instances.forEach(instance ->{
            try {
                CanalConnector connector = CanalConnectors.newClusterConnector("127.0.0.1:2181", instance, "", "");
                connector.connect();
                try{
                    connector.subscribe();
                    connector.rollback();
                    PipelineExecutor executor = new PipelineExecutor(connector, instance, canalContext.getInstanceTopics().get(instance),kafkaProducer);
                    executors.submit(executor);
                }catch (Exception e){
                    LOGGER.error("Connect to canal failed!");
                    connector.disconnect();
                    throw e;
                }
            }catch (Exception e){
                LOGGER.error("Build canal instance connection of " + instance +" failed, with exception {}", e.getMessage());
            }
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
            //旧的执行器全部停止之后清空清空执行器池
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Disconnect from old connection with exception {}", e.getMessage());
        }
        pipelineExecutors.clear();
        LOGGER.info("All connection has disconnected!");
    }
}
