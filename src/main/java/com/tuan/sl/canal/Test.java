package com.tuan.sl.canal;

import com.tuan.sl.canal.client.CanalConfigBuilder;
import com.tuan.sl.canal.executor.DataPipelineExecute;

public class Test {

    public static void main(String[] args) {

        CanalConfigBuilder builder = new CanalConfigBuilder();
        builder.build("127.0.0.1",11111,"example","","");

        DataPipelineExecute execute = new DataPipelineExecute(builder.getConfig());
        execute.execute();
    }
}
