/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import ai.onnxruntime.OrtSession.Result;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OrtTensorDecoder;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.apache.wayang.ml.encoding.TreeDecoder;
import org.apache.wayang.ml.encoding.TreeNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.HashSet;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.Arrays;

public class OrtMLModel {

    private static OrtMLModel INSTANCE;

    private OrtSession session;
    private OrtEnvironment env;

    private final Map<String, OnnxTensor> inputMap = new HashMap<>();
    private final Set<String> requestedOutputs = new HashSet<>();

    public static OrtMLModel getInstance(Configuration configuration) throws OrtException {
        if (INSTANCE == null) {
            INSTANCE = new OrtMLModel(configuration);
        }

        return INSTANCE;
    }

    private OrtMLModel(Configuration configuration) throws OrtException {
        this.loadModel(configuration.getStringProperty("wayang.ml.model.file"));
    }

    private void loadModel(String filePath) throws OrtException {
        if (this.env == null) {
            this.env = OrtEnvironment.getEnvironment("org.apache.wayang.ml");
            this.env.setTelemetry(false);
        }

        if (this.session == null) {
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            options.setInterOpNumThreads(16);
            options.setIntraOpNumThreads(16);
            this.session = env.createSession(filePath, options);
        }
    }

    // Just here as placeholder
    public double runModel(long[] encoded) {
        return 0;
    }

    /**
     * Close the session after running, {@link #closeSession()}
     * @param encodedVector
     * @return NaN on error, and a predicted cost on any other value.
     * @throws OrtException
     */
    public double runModel(
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1
    ) throws OrtException {
        double costPrediction;

        Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();

        float[][][] input1Left = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        long[][][] input1Right = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];

        //input1Left = input1.field0.toArray(input1Left);
        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                input1Left[0][i][j] = Long.valueOf(
                    input1.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            input1Right[0][i]  = input1.field1.get(0)[i];
        }

        OnnxTensor tensorOneLeft = OnnxTensor.createTensor(env, input1Left);
        OnnxTensor tensorOneRight = OnnxTensor.createTensor(env, input1Right);

        this.inputMap.put("input1", tensorOneLeft);
        this.inputMap.put("input2", tensorOneRight);

        this.requestedOutputs.add("output");

        BiFunction<Result, String, Float> unwrapFunc = (r, s) -> {
            try {
                return ((float[]) r.get(s).get().getValue())[0];
            } catch (OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return Float.NaN;
            }
        };

        try (Result r = session.run(inputMap, requestedOutputs)) {
            costPrediction = unwrapFunc.apply(r, "output");
        } catch(Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }

        return costPrediction;
    }

    public int runPairwise(
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1,
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input2
    ) throws OrtException {


        Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();
        long[] input3Dims = ((TensorInfo) inputInfoList.get("input3").getInfo()).getShape();
        long[] input4Dims = ((TensorInfo) inputInfoList.get("input4").getInfo()).getShape();

        float[][][] input1Left = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        long[][][] input1Right = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];
        float[][][] input2Left = new float[1][(int) input3Dims[1]][(int) input3Dims[2]];
        long[][][] input2Right = new long[1][(int) input4Dims[1]][(int) input4Dims[2]];

        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                input1Left[0][i][j] = Long.valueOf(
                    input1.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            input1Right[0][i]  = input1.field1.get(0)[i];
        }

        for (int i = 0; i < input2.field0.get(0).length; i++) {
            for (int j = 0; j < input2.field0.get(0)[i].length; j++) {
                input2Left[0][i][j] = Long.valueOf(
                    input2.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input2.field1.get(0).length; i++) {
            input2Right[0][i]  = input2.field1.get(0)[i];
        }

        OnnxTensor tensorOneLeft = OnnxTensor.createTensor(env, input1Left);
        OnnxTensor tensorOneRight = OnnxTensor.createTensor(env, input1Right);
        OnnxTensor tensorTwoLeft = OnnxTensor.createTensor(env, input2Left);
        OnnxTensor tensorTwoRight = OnnxTensor.createTensor(env, input2Right);

        this.inputMap.put("input1", tensorOneLeft);
        this.inputMap.put("input2", tensorOneRight);
        this.inputMap.put("input3", tensorTwoLeft);
        this.inputMap.put("input4", tensorTwoRight);

        this.requestedOutputs.add("output");

        BiFunction<Result, String, Float[]> unwrapFunc = (r, s) -> {
            try {
                float[] result = ((float[]) r.get(s).get().getValue());
                Float[] convResult = new Float[result.length];

                for (int i = 0; i < result.length; i++) {
                    convResult[i] = result[i];
                }

                return convResult;
            } catch (OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                e.printStackTrace();
                return new Float[]{Float.NaN};
            }
        };

        try (Result r = session.run(this.inputMap, this.requestedOutputs)) {
            Float[] result = unwrapFunc.apply(r, "output");

            System.out.println("[ML FLOAT RESULT]: " + result[0]);
            return Math.round(result[0]);
        } catch (OrtException e) {
            e.printStackTrace();

            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }
    }

    public Tuple<WayangPlan, TreeNode> runVAE(
        WayangPlan plan,
        TreeNode encoded
    ) throws OrtException {
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input = OrtTensorEncoder.encode(encoded);
        Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();

        float[][][] input1Left = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        long[][][] input1Right = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];

        //input1Left = input1.field0.toArray(input1Left);
        for (int i = 0; i < input.field0.get(0).length; i++) {
            for (int j = 0; j < input.field0.get(0)[i].length; j++) {
                input1Left[0][i][j] = Long.valueOf(
                    input.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input.field1.get(0).length; i++) {
            input1Right[0][i]  = input.field1.get(0)[i];
        }

        OnnxTensor tensorOneLeft = OnnxTensor.createTensor(env, input1Left);
        OnnxTensor tensorOneRight = OnnxTensor.createTensor(env, input1Right);
        OrtTensorDecoder decoder = new OrtTensorDecoder();

        this.inputMap.put("input1", tensorOneLeft);
        this.inputMap.put("input2", tensorOneRight);

        this.requestedOutputs.add("output");

        BiFunction<Result, String, float[][][]> unwrapFunc = (r, s) -> {
            try {
                return ((float[][][]) r.get(s).get().getValue());
            } catch (OrtException e) {
                e.printStackTrace();
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return null;
            }
        };

        try (Result r = session.run(inputMap, requestedOutputs)) {
            float[][][] resultTensor = unwrapFunc.apply(r, "output");
            System.out.println("Result: " + Arrays.deepToString(resultTensor));
            long[][][] longResult = new long[1][(int) resultTensor[0].length][(int) resultTensor[0][0].length];
            for (int i = 0; i < resultTensor[0].length; i++)  {
                for (int j = 0; j < resultTensor[0][i].length; j++) {
                    // Just shift the decimal point
                    longResult[0][i][j] = (long) (resultTensor[0][i][j] * 1_000_000_000);
                }
            }

            ArrayList<long[][]> mlResult = new ArrayList<long[][]>();
            mlResult.add(longResult[0]);
            Tuple<ArrayList<long[][]>, ArrayList<long[][]>> decoderInput = new Tuple<>(mlResult, input.field1);
            TreeNode decoded = decoder.decode(decoderInput);
            decoded.softmax();

            // Now set the platforms on the wayangPlan
            encoded = encoded.withPlatformChoicesFrom(decoded);

            WayangPlan decodedPlan = TreeDecoder.decode(encoded);

            return new Tuple<WayangPlan, TreeNode>(decodedPlan, encoded);
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
            //return new Tuple<WayangPlan, TreeNode>(plan, encoded);
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
            this.closeSession();
        }
    }

    /**
     * Closes the OrtModel resource, relinquishing any underlying resources.
     * @throws OrtException
     */
    public void closeSession() throws OrtException {
        this.session.close();
        this.env.close();
    }
}
