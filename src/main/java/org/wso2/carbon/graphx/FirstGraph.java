package org.wso2.carbon.graphx;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.storage.StorageLevel;

public class FirstGraph {

    public  static void main(String[] args) {
        SparkConf configuration = new SparkConf()
                .setAppName("FirstGraph")
                .setMaster("local");

        SparkContext sc = new SparkContext(configuration);
        String path = "/home/wso2/graph_data.txt";
        boolean canonicalOrientation = false;
        int numEdgePartitions = -1;
        StorageLevel edgeStorageLevel = StorageLevel.MEMORY_ONLY();
        StorageLevel vertexStorageLevel = StorageLevel.MEMORY_ONLY();

        Graph graph = GraphLoader.edgeListFile(sc, path, canonicalOrientation, numEdgePartitions, edgeStorageLevel, vertexStorageLevel);
        System.out.print(graph.vertices().count());
        System.out.print(graph.edges().count());
    }
}

