package com.lpy.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
/**
 * @ClassName: UDAFChooseOne
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/

/**
 *
 * 对于给定的分组，从分组中返回一个数组。当你
 *  想要将每个键与任意值关联，或者
 *  确保分组中的每个项目都相同。
 *
 *  这代替了UDAFFirst。
 *
 *  请注意，此功能不保证您将获得第一个
 *  项目，只有您将获得其中一项。
 *
 *   TODO：使它与Hive STRUCT类型一起使用。
 *
 */
@Description(name = "chooseone", value="_FUNC_(value) -从组中返回任意值")
public class UDAFChooseOne extends AbstractGenericUDAFResolver {

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("Only one paramter expected, but you provided " + parameters.length);
        }
        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {
        ObjectInspector inputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            inputOI = parameters[0];
            return ObjectInspectorUtils.getStandardObjectInspector(inputOI);
        }

        static class State implements AggregationBuffer {
            Object state = null;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
            State s = (State) agg;
            if (s.state == null) {
                s.state = ObjectInspectorUtils.copyToStandardObject(input[0], inputOI);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            State s = (State) agg;
            if (s.state == null) {
                s.state = ObjectInspectorUtils.copyToStandardObject(partial, inputOI);
            }
        }

        @Override
        public void reset(AggregationBuffer agg) {
            State s = (State) agg;
            s.state = null;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            State s = (State) agg;
            return s.state;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            State s = (State) agg;
            return s.state;
        }
    }

}
