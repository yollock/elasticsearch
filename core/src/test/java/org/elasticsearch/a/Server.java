package org.elasticsearch.a;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;

public class Server extends ESIntegTestCase {

    protected void createIndex() {
        createIndex(getConcreteIndexName());
    }

    protected String getConcreteIndexName() {
        return "yoll_index";
    }

    @Test
    public void testIndexActions() throws Exception {
        createIndex();
        NumShards numShards = getNumShards(getConcreteIndexName());
        logger.info("Running Cluster Health");
        ensureGreen();
        logger.info("Indexing [type1/1]");

        // 插入并索引数据
        // source("1", "test") = XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
        // 第一个是id, 第二个是name, 都是document的内容
        // 路径 : index/type/document
        IndexResponse indexResponse = client()//
            .prepareIndex()//
            .setIndex("yoll_index") //
            .setType("yoll_type").setId("1") //
            .setSource(source("1", "test")) //
            .setRefresh(true) //
            .execute() //
            .actionGet();


        Thread.sleep(Integer.MAX_VALUE);
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
