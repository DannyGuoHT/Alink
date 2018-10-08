package com.alibaba.alink.common.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import java.util.Arrays;

public class KeepColNamesManager {
    private TableSchema modelSchema;
    private TableSchema dataSchema;
    private AlinkParameter params;

    private String[] keepColNames;
    private TypeInformation[] keepTypes = null;
    private int keepN = -1;
    private int[] keepIdx = null;

    /**
     * a - b
     * @param a
     * @param b
     * @return
     */
    private String[] subtract(String[] a, String[] b) {
        if(null == a || a.length == 0 || null == b || b.length == 0)
            return a;

        int[] flag = new int[a.length];
        int num = 0;
        for (int i = 0; i < flag.length; i++) {
            if(TableUtil.findIndexFromName(b, a[i]) < 0) {
                flag[i] = 1;
                num++;
            } else {
                flag[i] = 0;
            }
        }

        String[] ret = new String[num];
        num = 0;
        for (int i = 0; i < flag.length; i++) {
            if(flag[i] == 1)
                ret[num++] = a[i];
        }
        return ret;
    }

    public KeepColNamesManager(TableSchema modelSchema, TableSchema dataSchema,
                               String[] outputColNames, AlinkParameter params) {
        this.modelSchema = modelSchema;
        this.dataSchema = dataSchema;
        this.params = params;

        this.keepColNames = params.getStringArrayOrDefault(ParamName.keepColNames, null);

        if (this.keepColNames != null) {
            if(outputColNames != null) {
                // check whether there are conflicts between keepColNames and outputColNames
                for(String outputColName : outputColNames) {
                    if(TableUtil.findIndexFromName(this.keepColNames, outputColName) >= 0)
                        throw new IllegalArgumentException("keepColNames conflict with output col name: " + outputColName);
                }
            }

            this.keepN = this.keepColNames.length;
            this.keepIdx = new int[this.keepN];
            keepN = 0;
            for (int i = 0; i < keepColNames.length; ++i) {
                int idx = TableUtil.findIndexFromName(dataSchema.getColumnNames(), keepColNames[i]);
                if (idx >= 0) {
                    keepIdx[keepN] = idx;
                    keepN++;
                }
            }
            if(keepN < keepColNames.length) {
                this.keepIdx = Arrays.copyOfRange(this.keepIdx, 0, keepN);
                for (int i = 0; i < keepN; i++) {
                    this.keepColNames[i] = dataSchema.getColumnNames()[this.keepIdx[i]];
                }
                this.keepColNames = Arrays.copyOfRange(this.keepColNames, 0, keepN);
            }
        } else {
            String[] toKeep = subtract(dataSchema.getColumnNames(), outputColNames);
            this.keepColNames = toKeep;
            this.keepN = toKeep.length;
            this.keepIdx = new int[this.keepN];
            for (int i = 0; i < keepN; ++i) {
                keepIdx[i] = TableUtil.findIndexFromName(dataSchema.getColumnNames(), keepColNames[i]);
            }
        }

        keepTypes = new TypeInformation[keepN];
        for (int i = 0; i < keepN; i++) {
            keepTypes[i] = dataSchema.getTypes()[keepIdx[i]];
        }
    }

    public String[] getKeepColNames() {
        return keepColNames;
    }

    public TypeInformation[] getKeepColTypes() {
        return keepTypes;
    }

    public int getKeepColSize() {
        return keepColNames.length;
    }

    public int[] getKeepColIndices() {
        return keepIdx;
    }
}