package org.apache.wayang.ml.encoding;

import com.google.common.primitives.Ints;
import org.apache.wayang.core.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;

public class OrtTensorDecoder {
    private HashMap<Integer, Node<int[]>> nodeToIDMap = new HashMap<>();

    public static void main(String[] args) {
        OrtTensorEncoder testo = new OrtTensorEncoder();

        Node<int[]> n1 = new Node<>(new int[]{2, 3},null,null);
        Node<int[]> n2 = new Node<>(new int[]{1, 2},null,null);
        Node<int[]> n3 = new Node<>(new int[]{-3,0},n1,n2);

        Node<int[]> n4 = new Node<>(new int[]{0, 1},null,null);
        Node<int[]> n5 = new Node<>(new int[]{-1, 0},null,null);
        Node<int[]> n6 = new Node<>(new int[]{1,2},n4,n5);

        Node<int[]> n7 = new Node<>(new int[]{0,1},n6,n3);

        ArrayList<Node<int[]>> testArr = new ArrayList<>();
        testArr.add(n7);

        Tuple<ArrayList<int[][]>, ArrayList<int[][]>> t = testo.prepareTrees(testArr);

        OrtTensorDecoder testo2 = new OrtTensorDecoder();
        System.out.println(testo2.decode(t));
    }

    //TODO: figure out output structure, from ml model
    /**
     * Decodes the output from a tree based NN model
     * @param mlOutput takes the out put from @
     */
    Node<?> decode(Tuple<ArrayList<int[][]>,ArrayList<int[][]>> mlOutput){
        ArrayList<int[][]> valueStructure = mlOutput.field0;
        ArrayList<int[][]> indexStructure = mlOutput.field1;

        for (int i = 0; i < valueStructure.size(); i++) { //iterate for each tree, in practice should only be 1
            int[][] values      = valueStructure.get(i);
            int[][] indexedTree = indexStructure.get(i);
            int[] flatIndexTree = Arrays.stream(indexedTree).reduce(Ints::concat).orElseThrow();

            for (int j = 0; j < flatIndexTree.length; j+=3) {
                int curID = flatIndexTree[j];
                int lID   = flatIndexTree[j+1];
                int rID   = flatIndexTree[j+2];

                int[] value = Arrays.stream(values)
                        .flatMapToInt(arr -> IntStream.of(arr[curID]))
                        .toArray();

                //fetch l,r from map such that we can reference values.
                Node<int[]> l       = nodeToIDMap.containsKey(lID)   ? nodeToIDMap.get(lID)   : new Node<>();
                Node<int[]> r       = nodeToIDMap.containsKey(rID)   ? nodeToIDMap.get(rID)   : new Node<>();
                Node<int[]> curNode = nodeToIDMap.containsKey(curID) ? nodeToIDMap.get(curID) : new Node<>(value, l, r);

                //set values
                curNode.value = value;
                curNode.l     = l;
                curNode.r     = r;

                //put values back into map so we can look them up in next loop
                nodeToIDMap.put(curID,curNode);
                nodeToIDMap.put(lID,l);
                nodeToIDMap.put(rID,r);
            }
        }
        return this.nodeToIDMap.get(1);
    }
}
