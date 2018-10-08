package com.alibaba.alink.common.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.matrix.VectorOp;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * An implementation of the Stochastic Outlier Selection algorithm by Jeroen Jansen
 * <p>
 * For more information about SOS, see https://github.com/jeroenjanssens/sos
 * J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. Stochastic
 * Outlier Selection. Technical Report TiCC TR 2012-001, Tilburg University,
 * Tilburg, the Netherlands, 2012.
 */

public class SOS {
    private double perplexity;
    private final int maxIter = 100;
    private final double tol = 1.0e-2;

    public SOS(AlinkParameter params) {
        perplexity = params.getDoubleOrDefault("perplexity", 4.0);
    }

    public static class ComputeDissimMatrixOp extends RichMapFunction<Tuple2<Integer, DenseVector>, Tuple2<Integer, DenseVector>> {
        private List<Tuple2<Integer, DenseVector>> data = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            data = getRuntimeContext().getBroadcastVariable("data");
        }

        @Override
        public Tuple2<Integer, DenseVector> map(Tuple2<Integer, DenseVector> value) throws Exception {
            int count = data.size();
            DenseVector row = DenseVector.zeros(count);

            for (Tuple2<Integer, DenseVector> sample : data) {
                int id = sample.f0;
                row.set(id, VectorOp.minus(value.f1, sample.f1).l2normSquare());
            }
            return new Tuple2<>(value.f0, row);
        }
    }

    /**
     * compute outlier probabilities for each sample
     */
    public DataSet<Tuple2<Integer, Double>> run(DataSet<Tuple2<Integer, DenseVector>> data) throws Exception {

        DataSet<Tuple2<Integer, DenseVector>> bc = data.map(new MapFunction<Tuple2<Integer, DenseVector>, Tuple2<Integer, DenseVector>>() {
            @Override
            public Tuple2<Integer, DenseVector> map(Tuple2<Integer, DenseVector> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1);
            }
        });

        // (1) compute dissimilarity matrix
        DataSet<Tuple2<Integer, DenseVector>> dissimMatrix = data
                .rebalance()
                .map(new ComputeDissimMatrixOp())
                .withBroadcastSet(bc, "data")
                .rebalance();

        // (2) compute the affinity matrix and then the binding probability
        // Tuple2: nodeId, bindingProbabilities
        final DataSet<Tuple2<Integer, DenseVector>> bindingProb = dissimMatrix
                .map(new ComputeBindingProbabilityOp(perplexity, maxIter, tol));

        // (3) compute outlier probability
        DataSet<Tuple2<Integer, Double>> outlierProb = bindingProb
                .<Tuple2<Integer, Double>>flatMap(new FlatMapFunction<Tuple2<Integer, DenseVector>, Tuple2<Integer, Double>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, DenseVector> in, Collector<Tuple2<Integer, Double>> collector) throws Exception {
                        final double threshold = 0.025;
                        for (int i = 0; i < in.f1.size(); i++) {
                            if (in.f0 == i) { // to avoid missing the point
                                collector.collect(new Tuple2<>(i, 0.));
                            } else {
                                double v = in.f1.get(i);
                                if (v > threshold)
                                    collector.collect(new Tuple2<>(i, v));
                            }
                        }
                    }
                })
                .groupBy(0)
                .<Tuple2<Integer, Double>>reduceGroup(new GroupReduceFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Double>> iterable, Collector<Tuple2<Integer, Double>> collector) throws Exception {
                        final Iterator<Tuple2<Integer, Double>> bindingProbs = iterable.iterator();
                        double s = 1.0;
                        int nodeId = -1;
                        while (bindingProbs.hasNext()) {
                            Tuple2<Integer, Double> p = bindingProbs.next();
                            nodeId = p.f0;
                            s *= (1.0 - p.f1);
                        }
                        collector.collect(new Tuple2<>(nodeId, s));
                    }
                });

        return outlierProb;
    }

    /**
     * compute the beta (1.0/sigma^2) and \sum_k a_ik for each sample
     *
     * @param data
     * @return
     * @throws Exception TODO: remove outliers in the final model
     */
    public DataSet<Tuple3<Integer, Double, Double>> train(DataSet<Tuple2<Integer, DenseVector>> data) throws Exception {

        DataSet<Tuple2<Integer, DenseVector>> bc = data.map(new MapFunction<Tuple2<Integer, DenseVector>, Tuple2<Integer, DenseVector>>() {
            @Override
            public Tuple2<Integer, DenseVector> map(Tuple2<Integer, DenseVector> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1);
            }
        });

        // (1) compute dissimilarity matrix
        DataSet<Tuple2<Integer, DenseVector>> dissimMatrix = data
                .map(new ComputeDissimMatrixOp())
                .withBroadcastSet(bc, "data");

        // (2) compute the affinity matrix and then the binding probability
        // Tuple2: nodeId, bindingProbabilities
        final double tol = this.tol;
        final double h = this.perplexity;
        final int maxIter = this.maxIter;
        DataSet<Tuple3<Integer, Double, Double>> beta = dissimMatrix
                .map(new MapFunction<Tuple2<Integer, DenseVector>, Tuple3<Integer, Double, Double>>() {
                    @Override
                    public Tuple3<Integer, Double, Double> map(Tuple2<Integer, DenseVector> value) throws Exception {
                        double beta = solveForBeta(value.f0, value.f1, 1.0, h, maxIter, tol);
                        double s = 0.;

                        // compute the affinity
                        for (int i = 0; i < value.f1.size(); i++) {
                            if (i == value.f0)
                                continue;
                            double v = value.f1.get(i);
                            v = Math.exp(-v * beta);
                            s += v;
                        }
                        return new Tuple3<>(value.f0, beta, s);
                    }
                });

        return beta;
    }

    public static final class ComputeBindingProbabilityOp implements MapFunction<Tuple2<Integer, DenseVector>, Tuple2<Integer, DenseVector>> {
        private double h; // the perplexity
        private int maxIter;
        private double tol;

        public ComputeBindingProbabilityOp(double h, int maxIter, double tol) {
            this.h = h;
            this.maxIter = maxIter;
            this.tol = tol;
        }

        @Override
        public Tuple2<Integer, DenseVector> map(Tuple2<Integer, DenseVector> row) throws Exception {
            // beta: 1 / (2 * sigma^2)
            // compute beta by solving a nonlinear equation
            double beta = solveForBeta(row.f0, row.f1, 1.0, this.h, this.maxIter, this.tol);
            DenseVector ret = new DenseVector(row.f1.size());
            double s = 0.;

            // compute the affinity
            for (int i = 0; i < row.f1.size(); i++) {
                if (i == row.f0) {
                    ret.set(i, 0.);
                }
                double v = row.f1.get(i);
                v = Math.exp(-v * beta);
                s += v;
                ret.set(i, v);
            }

            // compute the binding probability
            ret.scaleEqual(1.0 / s);

            return new Tuple2<>(row.f0, ret);
        }
    }

    private static double solveForBeta(int idx, DenseVector d, double beta0, double h, int maxIter, double tol) {
        int iter = 0;
        double beta = beta0;
        double logh = Math.log(h);
        double hCurr = computeLogH(idx, d, beta);
        double err = hCurr - logh;
        double betaMin = 0.;
        double betaMax = Double.MAX_VALUE;

        while (iter < maxIter && (Double.isNaN(err) || Math.abs(err) > tol)) {
            if (Double.isNaN(err)) {
                beta = beta / 10.0;
            } else {
                if (err > 0.) { // should increase beta
                    if (betaMax == Double.MAX_VALUE) {
                        betaMin = beta;
                        beta *= 2;
                    } else {
                        betaMin = beta;
                        beta = 0.5 * (betaMin + betaMax);
                    }
                } else { // should decrease beta
                    betaMax = beta;
                    beta = 0.5 * (betaMin + beta);
                }
            }
            iter++;

            hCurr = computeLogH(idx, d, beta);
            err = hCurr - logh;
        }

        return beta;
    }

    private static double computeLogH(int idx, DenseVector d, double beta) {
        double[] affinity = new double[d.size()];
        double sumA = 0.;
        for (int i = 0; i < affinity.length; i++) {
            if (i == idx) {
                affinity[i] = 0.;
                continue;
            }
            affinity[i] = Math.exp(-beta * d.get(i));
            sumA += affinity[i];
        }

        double logh = 0.;
        for (int i = 0; i < affinity.length; i++) {
            double v = affinity[i] * d.get(i);
            logh += v;
        }
        logh *= (beta / sumA);
        logh += Math.log(sumA);

        return logh;
    }
}
