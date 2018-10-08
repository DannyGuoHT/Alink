package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;

/**
 * Set the output colNames and types
 */

public class OutputTableInfo implements Serializable {
    private String[] outColNames;
    private TypeInformation[] outTypes;
    private String selectedColNames;
    private String leftSelectedColNames;
    private String rightSelectedColNames;

    public String[] getOutColNames() {
        return this.outColNames;
    }

    public TypeInformation[] getOutTypes() {
        return this.outTypes;
    }

    public String getSelectedColNames() {
        return this.selectedColNames;
    }

    public String getLeftSelectedColNames() {
        return this.leftSelectedColNames;
    }

    public String getRightSelectedColNames() {
        return this.rightSelectedColNames;
    }

    /**
     * For the case of one table input, calculate the similarity in pair.
     */
    public void getOutputTableInfo(String[] colNames, TypeInformation[] types, String selectedColName0, String selectedColName1,
                                   String outputColName, String[] keepColNames) {
        int indexCol0 = TableUtil.findIndexFromName(colNames, selectedColName0);
        int indexCol1 = TableUtil.findIndexFromName(colNames, selectedColName1);

        if (indexCol0 < 0) {
            throw new RuntimeException(selectedColName0 + " not exists!");
        }

        if (indexCol1 < 0) {
            throw new RuntimeException(selectedColName1 + " not exists!");
        }

        this.outColNames = new String[1 + keepColNames.length];
        this.outTypes = new TypeInformation[1 + keepColNames.length];

        this.outColNames[0] = outputColName;
        this.outTypes[0] = Types.DOUBLE;

        for (int i = 0; i < keepColNames.length; i++) {
            int index = TableUtil.findIndexFromName(colNames, keepColNames[i]);
            if (index < 0) {
                throw new RuntimeException(keepColNames[i] + " not exists!");
            }
            this.outColNames[i + 1] = keepColNames[i];
            this.outTypes[i + 1] = types[index];
        }

        StringBuilder selectedColNames = new StringBuilder(selectedColName0 + " as input_col0, " + selectedColName1 + " as input_col1");
        for (int i = 0; i < keepColNames.length; i++) {
            selectedColNames.append(", ").append(keepColNames[i]);
        }
        this.selectedColNames = selectedColNames.toString();
    }

    /**
     * For the case of two table inputs, calculate the topN similarity.
     */
    public void getOutputTableInfo(String[] leftColNames, TypeInformation[] leftTypes, String[] rightColNames,
                                   TypeInformation[] rightTypes, String inputSelectedColName, String mapSelectedColName,
                                   String outputColName, String[] leftKeepColNames, String[] rightKeepColNames) {
        this.outColNames = new String[1 + leftKeepColNames.length + rightKeepColNames.length];
        this.outTypes = new TypeInformation[1 + leftKeepColNames.length + rightKeepColNames.length];
        this.outColNames[0] = outputColName;
        this.outTypes[0] = Types.DOUBLE;

        if (inputSelectedColName == null) {
            for (int i = 0; i < leftTypes.length; i++) {
                if (leftTypes[i].equals(Types.STRING)) {
                    inputSelectedColName = leftColNames[i];
                    break;
                }
            }
        } else {
            if (TableUtil.findIndexFromName(leftColNames, inputSelectedColName) < 0) {
                throw new RuntimeException(inputSelectedColName + " not exists!");
            }
        }

        for (int i = 0; i < leftKeepColNames.length; i++) {
            int index = TableUtil.findIndexFromName(leftColNames, leftKeepColNames[i]);
            if (index < 0) {
                throw new RuntimeException(leftKeepColNames[i] + " not exists!");
            }
            this.outColNames[1 + i] = "LeftTable_" + leftKeepColNames[i];
            this.outTypes[1 + i] = leftTypes[index];
        }

        if (mapSelectedColName == null) {
            for (int i = 0; i < rightTypes.length; i++) {
                if (rightTypes[i].equals(Types.STRING)) {
                    mapSelectedColName = rightColNames[i];
                    break;
                }
            }
        } else {
            if (TableUtil.findIndexFromName(rightColNames, mapSelectedColName) < 0) {
                throw new RuntimeException(mapSelectedColName + " not exists!");
            }
        }

        for (int i = 0; i < rightKeepColNames.length; i++) {
            int index = TableUtil.findIndexFromName(rightColNames, rightKeepColNames[i]);
            if (index < 0) {
                throw new RuntimeException(rightKeepColNames[i] + " not exists!");
            }
            this.outColNames[1 + leftKeepColNames.length + i] = "RightTable_" + rightKeepColNames[i];
            this.outTypes[1 + leftKeepColNames.length + i] = rightTypes[index];
        }

        StringBuilder leftSelectedColNames = new StringBuilder(inputSelectedColName + " as input_col");
        for (int i = 0; i < leftKeepColNames.length; i++) {
            leftSelectedColNames.append(", ").append(leftKeepColNames[i]);
        }
        this.leftSelectedColNames = leftSelectedColNames.toString();

        StringBuilder rightSelectedColNames = new StringBuilder(mapSelectedColName + " as input_col");
        for (int i = 0; i < rightKeepColNames.length; i++) {
            rightSelectedColNames.append(", ").append(rightKeepColNames[i]);
        }
        this.rightSelectedColNames = rightSelectedColNames.toString();
    }
}
