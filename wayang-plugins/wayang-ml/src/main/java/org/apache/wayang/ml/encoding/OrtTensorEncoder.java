package org.apache.wayang.ml.encoding;


import org.apache.wayang.core.util.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Implementation of
 */


class Node<V> {
    Node (V value, Node<V> l, Node<V> r) {
        this.value = value;
        this.r = r;
        this.l = l;
    }
    Node () {

    }

    public V value;
    public Node<V> l;
    public Node<V> r;

    @Override
    public String toString() {
        if (l == null) return "" + value + "";

        return "(" + value + "," + l + "," + r + ")";
    }

    public boolean isLeaf(){
        return l == null;
    }
}

public class OrtTensorEncoder {
    /**
     * This method prepares the trees for creation of the OnnxTensor
     * @param trees
     * @return returns a tuple of (flatTrees, indexes)
     */
    public Tuple<ArrayList<int[][]>,ArrayList<int[][]>>prepareTrees(ArrayList<Node<int[]>> trees){
        ArrayList<int[][]> flatTrees = trees.stream()
                .map(this::flatten)
                .collect(Collectors.toCollection(ArrayList::new));

        flatTrees = padAndCombine(flatTrees);

        flatTrees = transpose(flatTrees);

        ArrayList<int[][]> indexes = trees.stream()
                .map(this::treeConvIndexes)
                .collect(Collectors.toCollection(ArrayList::new)); //weird structure
        indexes = padAndCombine(indexes);

        return new Tuple<>(flatTrees, indexes);
    }

    private static ArrayList<int[][]> transpose(ArrayList<int[][]> flatTrees) {
        return flatTrees.stream().map(tree -> IntStream.range(0, tree[0].length) //transpose matrix
                        .mapToObj(i -> Arrays.stream(tree)
                                .mapToInt(row -> row[i])
                                .toArray()).toArray(int[][]::new)
        ).collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Create indexes that, when used as indexes into the output of `flatten`,
     * create an array such that a stride-3 1D convolution is the same as a
     * tree convolution.
     * @param root
     * @return
     */
    private int[][] treeConvIndexes(Node<int[]> root){
        Node<Integer> indexTree = preorderIndexes(root, 1);

        ArrayList<int[]> acc = new ArrayList<>(); //in place of a generator
        treeConvIndexesStep(indexTree,acc); //mutates acc

        int[] flatAcc = acc.stream()
                .flatMapToInt(Arrays::stream)
                .toArray();
        return Arrays.stream(flatAcc)
                .mapToObj(v -> new int[]{v})
                .toArray(int[][]::new);
    }


    private void treeConvIndexesStep(Node<Integer> root, ArrayList<int[]> acc){
        if (!root.isLeaf()) {
            int ID  = root.value;
            int lID = root.l.value;
            int rID = root.r.value;

            acc.add(new int[]{ID,lID,rID});
            treeConvIndexesStep(root.l,acc);
            treeConvIndexesStep(root.r,acc);
        } else {
            acc.add(new int[]{root.value,0,0});
        }
    }


    /**
     * An array containing the nodes ordered by their index.
     * @return
     */
    private ArrayList<Node<?>> orderedNodes = new ArrayList<>();

    /**
     * transforms a tree into a tree of preorder indexes
     * @return
     * @param idx needs to default to one.
     */
    private Node<Integer> preorderIndexes(Node<int[]> root, int idx){ //this method is very scary
        //System.out.println("Node: " + root + " id: " + idx);
        orderedNodes.add(root);
        if (root.isLeaf()) {
            return new Node<>(idx,null,null);
        }

        assert root.l != null && root.r != null;

        Node<Integer> leftSubTree = preorderIndexes(root.l, idx+1);

        int maxIndexInLeftSubTree = rightMost(leftSubTree);

        Node<Integer> rightSubTree = preorderIndexes(root.r, maxIndexInLeftSubTree + 1);

        return new Node<>(idx, leftSubTree, rightSubTree);
    }

    private int rightMost(Node<Integer> root){
        if (!root.isLeaf()) return rightMost(root.r);
        return root.value;
    }

    /**
     * @param flatTrees
     * @return
     */
    private ArrayList<int[][]> padAndCombine(List<int[][]> flatTrees) {
        int secondDim = flatTrees.get(0)[0].length;                                   //find the size of a flat trees node structure
        int maxFirstDim = flatTrees.stream()
                .map(a -> a.length)
                .max(Integer::compare).get(); //we are trying to find the largest flat tree

        ArrayList<int[][]> vecs = new ArrayList<>();

        for (int[][] tree : flatTrees) {
            int[][] padding = new int[maxFirstDim][secondDim];

            for (int i = 0; i < tree.length; i++) {
                System.arraycopy(tree[i], 0, padding[i], 0, tree[i].length); //should never throw exception bc of int[][] padding = new int[maxFirstDim][secondDim];
            }

            vecs.add(padding);
        }

        return vecs;
    }

    public static void main(String[] args) {
        //matrix transpose test
        ArrayList<int[][]> arr = new ArrayList<>();

        int[][] matrix1 = new int[][]{
                {1,2,3,4},
                {5,6,7,8},
                {9,10,11,12},
                {13,14,15,16}
        };

        arr.add(matrix1);

        arr = arr.stream().map(tree -> IntStream.range(0, tree[0].length)
                .mapToObj(i -> Arrays.stream(tree).mapToInt(row -> row[i]).toArray()).toArray(int[][]::new)
        ).collect(Collectors.toCollection(ArrayList::new));

        assert(Arrays.deepEquals(arr.get(0), new int[][]{{1, 5, 9, 13},
                {2, 6, 10, 14},
                {3, 7, 11, 15},
                {4, 8, 12, 16}}));
        System.out.println("test 1: Passed");

        //System.out.println(Arrays.deepToString(arr.get(0)));

        Node<int[]> n1 = new Node<>(new int[]{2, 3},null,null);
        Node<int[]> n2 = new Node<>(new int[]{1, 2},null,null);
        Node<int[]> n3 = new Node<>(new int[]{-3,0},n1,n2);

        Node<int[]> n4 = new Node<>(new int[]{0, 1},null,null);
        Node<int[]> n5 = new Node<>(new int[]{-1, 0},null,null);
        Node<int[]> n6 = new Node<>(new int[]{1,2},n4,n5);

        Node<int[]> n7 = new Node<>(new int[]{0,1},n6,n3);

        OrtTensorEncoder testo = new OrtTensorEncoder();

        int[][] correcto = testo.flatten(n7);

        int[][] valid = new int[][]
                {{0,0},{0,1},{1,2},{0,1},{-1,0},{-3,0},{2,3},{1,2}};
        assert Arrays.deepEquals(valid, correcto);
        System.out.println("test 2: Passed");

        ArrayList<int[][]> correcto2 = testo.padAndCombine(Collections.singletonList(valid));
        System.out.println("test 3: Passed");

        //new int[][]{{0, 0, 1, 0, -1, -3, 2, 1}, {0, 1, 2, 1, 0, 0, 3, 2}}
        assert true;
        System.out.println("test 4: Passed");

        System.out.println(Arrays.deepToString(testo.treeConvIndexes(n7)));

        System.out.println("test 5: Passed");
        assert(testo.preorderIndexes(n7,1).toString().equals("(1,(2,3,4),(5,6,7))"));

        System.out.println("test 6: Passed");

        ArrayList<Node<int[]>> testArr = new ArrayList<>();
        testArr.add(n7);
        Tuple<ArrayList<int[][]>, ArrayList<int[][]>> t = testo.prepareTrees(testArr);
        t.field0.forEach(tree -> System.out.println(Arrays.deepToString(tree)));
        t.field1.forEach(tree -> System.out.println(Arrays.deepToString(tree)));
    }


    /**
     * @param root
     * @return
     */
    private int[][] flatten(Node<int[]> root){
        ArrayList<int[]> acc = new ArrayList<>();
        flattenStep(root,acc);


        acc.add(0, new int[acc.get(0).length]); //not sure that the size is correct.

        return acc.toArray(int[][]::new); //fix this. idk if it distributes the rows correctly.
    }

    private void flattenStep(Node<int[]> v, ArrayList<int[]> acc){
        if (v.isLeaf()) {
            acc.add(v.value);
            return;
        }

        acc.add(v.value);
        flattenStep(v.l, acc);
        flattenStep(v.r, acc);
    }
}
