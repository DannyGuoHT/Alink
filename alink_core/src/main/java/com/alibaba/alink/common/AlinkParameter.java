package com.alibaba.alink.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.common.AlinkSession.gson;

public class AlinkParameter implements Serializable {
    private HashMap <String, String> params;

    public AlinkParameter() {
        this.params = new HashMap <>();
    }

    public int size() {
        return params.size();
    }

    public void clear() {
        params.clear();
    }

    public boolean isEmpty() {
        return params.isEmpty();
    }

    public AlinkParameter clone() {
        AlinkParameter cloneAlinkParameter = new AlinkParameter();
        cloneAlinkParameter.params = (HashMap <String, String>) (this.params.clone());
        return cloneAlinkParameter;
    }

    public void remove(String paramName) {
        this.params.remove(paramName);
    }

    public AlinkParameter put(AlinkParameter otherParams) {
        if (null != otherParams) {
            for (Map.Entry <String, String> entry : otherParams.params.entrySet()) {
                this.params.put(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    public AlinkParameter putIgnoreNull(String paramName, Object paramValue) {
        if (null == paramValue) {
            return this;
        } else {
            return put(paramName, paramValue);
        }
    }

    public AlinkParameter put(String paramName, Object paramValue) {
        if (null == paramValue) {
            this.params.put(paramName, null);
        } else {
            this.params.put(paramName, gson.toJson(paramValue));
        }
        return this;
    }

    public AlinkParameter put(String paramName, Object paramValue, Class paramClass) {
        this.params.put(paramName, gson.toJson(paramValue, paramClass));
        return this;
    }

    public AlinkParameter put(Map <String, Object> otherParams) {
        if (null != otherParams) {
            for (Map.Entry <String, Object> entry : otherParams.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    public String toJson() {
        return gson.toJson(this.params);
    }

    public static AlinkParameter fromJson(String jsonString) {
        AlinkParameter ret = new AlinkParameter();
        ret.params = gson.fromJson(jsonString, ret.params.getClass());
        return ret;
    }

    public boolean contains(String paramName) {
        return params.containsKey(paramName);
    }

    public boolean contains(String[] paramNames) {
        if (null == paramNames) return true;
        for (String paramName : paramNames) {
            if (!params.containsKey(paramName)) {
                return false;
            }
        }
        return true;
    }

    public Set <String> listParamNames() {
        return params.keySet();
    }


    public <T> T get(String paramName, Class <T> classOfT) {
        if (!this.params.containsKey(paramName)) {
            throw new RuntimeException("Not have parameter : " + paramName);
        } else {
            String paramValue = this.params.get(paramName);
            try {
                return gson.fromJson(paramValue, classOfT);
            } catch (Exception ex) {
                throw new RuntimeException("Error in fromJson the paramValue : " + paramName + "\n" + ex.getMessage());
            }
        }
    }

    public <T> T getOrDefault(String paramName, Class <T> classOfT, Object defaultValue) {
        if (this.params.containsKey(paramName)) {
            return get(paramName, classOfT);
        } else {
            if (null == defaultValue) {
                return null;
            } else if (defaultValue.getClass().equals(classOfT)) {
                return (T) defaultValue;
            } else {
                throw new RuntimeException("Wrong class type of default value.");
            }
        }
    }

    public String getString(String paramName) {
        return get(paramName, String.class);
    }

    public String getStringOrDefault(String paramName, String defaultValue) {
        return getOrDefault(paramName, String.class, defaultValue);
    }

    @Override
    public String toString() {
        return "AlinkParameter" + params;
    }

    public Boolean getBool(String paramName) {
        return get(paramName, Boolean.class);
    }

    public Boolean getBoolOrDefault(String paramName, Boolean defaultValue) {
        return getOrDefault(paramName, Boolean.class, defaultValue);
    }

    public Integer getInteger(String paramName) {
        return get(paramName, Integer.class);
    }

    public Integer getIntegerOrDefault(String paramName, Integer defaultValue) {
        return getOrDefault(paramName, Integer.class, defaultValue);
    }

    public Long getLong(String paramName) {
        return get(paramName, Long.class);
    }

    public Long getLongOrDefault(String paramName, Long defaultValue) {
        return getOrDefault(paramName, Long.class, defaultValue);
    }

    public Double getDouble(String paramName) {
        return get(paramName, Double.class);
    }

    public Double getDoubleOrDefault(String paramName, Double defaultValue) {
        return getOrDefault(paramName, Double.class, defaultValue);
    }

    public Double[] getDoubleArray(String paramName) {
        return get(paramName, Double[].class);
    }

    public Double[] getDoubleArrayOrDefault(String paramName, Double[] defaultValue) {
        return getOrDefault(paramName, Double[].class, defaultValue);
    }

    public String[] getStringArray(String paramName) {
        return get(paramName, String[].class);
    }

    public String[] getStringArrayOrDefault(String paramName, String[] defaultValue) {
        return getOrDefault(paramName, String[].class, defaultValue);
    }

    public Integer[] getIntegerArray(String paramName) {
        return get(paramName, Integer[].class);
    }

    public Integer[] getIntegerArrayOrDefault(String paramName, Integer[] defaultValue) {
        return getOrDefault(paramName, Integer[].class, defaultValue);
    }

    public Long[] getLongArray(String paramName) {
        return get(paramName, Long[].class);
    }

    public Long[] getLongArrayOrDefault(String paramName, Long[] defaultValue) {
        return getOrDefault(paramName, Long[].class, defaultValue);
    }

}
