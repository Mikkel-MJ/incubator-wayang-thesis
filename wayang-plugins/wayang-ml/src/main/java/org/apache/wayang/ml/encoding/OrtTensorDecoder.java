package org.apache.wayang.ml.encoding;

import com.google.common.primitives.Ints;
import org.apache.wayang.core.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Supplier;

public class OrtTensorDecoder {
    Node<?> root;

    public static void main(String[] args) {

    }

    //TODO: figure out output structure, from ml model
    /**
     * Decodes the output from a tree based NN model
     * @param mlOutput takes the out put from @
     */
    void decode(Tuple<ArrayList<int[][]>,ArrayList<int[][]>> mlOutput){
        ArrayList<int[][]> valueStructure = mlOutput.field0;
        ArrayList<int[][]> indexStructure = mlOutput.field1;

        for (int i = 0; i < valueStructure.size(); i++) { //iterate for each tree, in practice should only be 1
            int[][] indexedTree = indexStructure.get(i);
            int[] flatIndexTree = Arrays.stream(indexedTree).reduce(Ints::concat).orElseThrow();

            for (int j = 0; j < flatIndexTree.length; j+=3) {
                Object lID   = flatIndexTree[j];
                Object rID   = flatIndexTree[j+1];
                Object curID = flatIndexTree[j+2];
            }
        }

    }

}
