package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.utils.JdbcTypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;

public abstract class MLModel extends AlinkModel {

    public static TableSchema getModelSchemaWithType(TypeInformation <?> type) {
        return AlinkModel.getModelSchemaWithInfo(ModelType.ML, JdbcTypeConverter.getSqlType(type).toLowerCase());
    }

    public static Tuple2 <TypeInformation <?>, Object[]> getLableTypeValues(AlinkParameter meta) {
        TypeInformation <?> lableType = JdbcTypeConverter.getFlinkType(meta.getString(ParamName.labelType));
        Object[] lableValues = null;

        if (meta.contains(ParamName.labelValues)) {
            Class lableArrayClassType = String[].class;
            switch (meta.getString(ParamName.labelType).toUpperCase()) {
                case "BOOLEAN":
                    lableArrayClassType = Boolean[].class;
                    break;
                case "TINYINT":
                case "BYTE":
                    lableArrayClassType = Byte[].class;
                    break;
                case "SMALLINT":
                case "SHORT":
                    lableArrayClassType = Short[].class;
                    break;
                case "INTEGER":
                    lableArrayClassType = Integer[].class;
                    break;
                case "BIGINT":
                case "LONG":
                    lableArrayClassType = Long[].class;
                    break;
                case "FLOAT":
                    lableArrayClassType = Float[].class;
                    break;
                case "DOUBLE":
                    lableArrayClassType = Double[].class;
                    break;
                default:
            }

            lableValues = (Object[]) meta.get(ParamName.labelValues, lableArrayClassType);
        }

        return new Tuple2 <TypeInformation <?>, Object[]>(lableType, lableValues);
    }


}
