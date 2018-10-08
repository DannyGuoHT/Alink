package com.alibaba.alink.common.utils.directreader;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.sink.DBSinkBatchOp;
import com.alibaba.alink.io.AlinkDB;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.MemDB;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class DirectReader implements Serializable {
    private static final String BUFFER_CONFIG_FILE_PATH = "buffer_conf.properties";

    public static final String ALINK_DIRECT_READ_PREFIX = "alink.direct.read";
    private static final String ALINK_DIRECT_READ_LOCAL = "alink.direct.read.local";
    private static final String ALINK_DIRECT_READ_EXTERNAL_WRITE = "alink.direct.read.web";
    private static final String ALINK_DIRECT_READ_DB_NAME_KEY = "alink.direct.read.db_name";

    private static final String ALINK_DIRECT_TMP_TABLE_PREFIX_KEY = "alink.direct.read.name_prefix";
    private static final String ALINK_DIRECT_TMP_TABLE_PREFIX = "tmp_alink";

    /**
     * db for direct read.
     */
    private AlinkDB alinkDB;
    /**
     * attention: when construct DirectReader, isLocal is changed from null to true/false.
     * environment of client is different with open function.
     */
    private Boolean isLocal;

    /**
     * when use external write, it can avoid execute at client.
     * it is useful at detached mode.
     */
    private Boolean isExternalWrite = false;

    private AlinkParameter configure;

    public DirectReader() {
        try {
            Boolean isLocal = AlinkSession.getExecutionEnvironment() instanceof LocalEnvironment;

            boolean isForceLocal = System.getProperty(ALINK_DIRECT_READ_LOCAL) != null;

            isLocal = isForceLocal ? isForceLocal : isLocal;

            isExternalWrite = System.getProperty(ALINK_DIRECT_READ_EXTERNAL_WRITE) != null;

            Properties properties = GlobalConfig.getGlobalConfigClient().getProperties() == null
                || GlobalConfig.getGlobalConfigClient().getProperties().isEmpty()?
                loadProperties() : GlobalConfig.getGlobalConfigClient().getProperties();

            configure = properties2AlinkParameter(properties);

            reload(isLocal);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List <Row> directRead(BatchOperator batchOperator) throws Exception {
        BatchStreamConnector batchStreamConnector = this.collect(batchOperator);
        return this.directRead(batchStreamConnector);
    }

    public List <Row> directRead(BatchStreamConnector batchStreamConnector) throws Exception {
        refreshConfigure(batchStreamConnector.getConfigure());
        reload(batchStreamConnector.getLocal());

        List <Row> ret = alinkDB.directRead(batchStreamConnector);

        return ret;
    }

    public List <Row> directRead(BatchStreamConnector batchStreamConnector,
                                 DistributedInfoProxy distributedInfoProxy) throws Exception {
        refreshConfigure(batchStreamConnector.getConfigure());
        reload(batchStreamConnector.getLocal());

        List <Row> ret = alinkDB.directRead(batchStreamConnector, distributedInfoProxy);

        return ret;
    }

    public static String genTmpTableName(String prefix) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        if (prefix == null) {
            prefix = ALINK_DIRECT_TMP_TABLE_PREFIX;
        }

        return (prefix + "_" + df.format(new Date()) + "_"
            + UUID.randomUUID()).replace('-', '_');
    }

    public BatchStreamConnector collect(BatchOperator batchOperator) throws Exception {
        String prefix = configure == null ? null : configure.getString(ALINK_DIRECT_TMP_TABLE_PREFIX_KEY);
        String tableName = genTmpTableName(prefix);

        if (isLocal) {
            List<Row> buffer = batchOperator.collect();
            return new BatchStreamConnector(tableName, buffer, configure);
        } else if (isExternalWrite) {
            return alinkDB.initDirectReadContext(
                new BatchStreamConnector(batchOperator.getTableName(), configure));
        } else {
            if (alinkDB.hasTable(tableName)) {
                alinkDB.dropTable(tableName);
            }

            DBSinkBatchOp dbSinkBatchOp = new DBSinkBatchOp(alinkDB, tableName);
            batchOperator.linkTo(dbSinkBatchOp);

            TmpTable.getTmpTablesClient().insertTableName(tableName);

            AlinkSession.getExecutionEnvironment().execute();

            return alinkDB.initDirectReadContext(
                new BatchStreamConnector(tableName, configure));
        }
    }

    private void reload(Boolean isLocal) throws Exception {
        if (this.isLocal == isLocal) {
            return;
        }

        this.isLocal = isLocal;

        alinkDB = load();
    }

    private void refreshConfigure(AlinkParameter configure) {
        this.configure = configure;
    }

    private static AlinkDB getMemDB(Properties properties) {
        return new MemDB(null);
    }

    private Properties loadProperties() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(BUFFER_CONFIG_FILE_PATH);

        if (is == null) {
            return null;
        }

        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }

    private static AlinkParameter properties2AlinkParameter(Properties properties) {
        if (properties == null) {
            return null;
        }

        AlinkParameter alinkParameter = new AlinkParameter();
        for (String name : properties.stringPropertyNames()) {
            alinkParameter.putIgnoreNull(name, properties.getProperty(name));
        }

        return alinkParameter;
    }

    private AlinkDB loadDB() throws Exception {
        return AnnotationUtils.createDB(configure.getString(ALINK_DIRECT_READ_DB_NAME_KEY),
            configure);
    }

    private AlinkDB load() throws Exception {
        if (isLocal) {
            return getMemDB(null);
        }

        return loadDB();
    }

    public static class BatchStreamConnector implements Serializable {
        private String tableName;
        private Boolean isLocal;

        /**
         * using mem buffer for local run.
         */
        private List<Row> buffer;

        /**
         * configure for connector.
         */
        private AlinkParameter configure;

        public BatchStreamConnector(String tableName, List <Row> buffer, AlinkParameter configure) {
            this.tableName = tableName;
            this.isLocal = true;
            this.buffer = buffer;
            this.configure = configure;
        }

        public BatchStreamConnector(String tableName, AlinkParameter configure) {
            this.tableName = tableName;
            this.isLocal = false;
            this.buffer = null;
            this.configure = configure;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public Boolean getLocal() {
            return isLocal;
        }

        public void setLocal(Boolean local) {
            isLocal = local;
        }

        public List <Row> getBuffer() {
            return buffer;
        }

        public void setBuffer(List <Row> buffer) {
            this.buffer = buffer;
        }

        public AlinkParameter getConfigure() {
            return configure;
        }

        public void setConfigure(AlinkParameter configure) {
            this.configure = configure;
        }
    }

    public static class TmpTable implements Serializable {
        private List <String> tableLists = new ArrayList <>();

        private static class TmpTableInstance {
            private static final TmpTable tmpTable = new TmpTable();
        }

        private TmpTable() {
        }

        public static TmpTable getTmpTablesClient() {
            return TmpTableInstance.tmpTable;
        }

        public synchronized void insertTableName(String tableName) {
            tableLists.add(tableName);
        }

        public synchronized void cleanAll(AlinkDB db) throws Exception {
            for (String tableName : tableLists) {
                db.dropTable(tableName);
            }

            tableLists.clear();
        }
    }


    public static class GlobalConfig implements Serializable {
        private Properties properties = new Properties();

        private static class GlobalConfigInstance {
            private static final GlobalConfig globalConfig = new GlobalConfig();
        }

        private GlobalConfig() {
        }

        public static GlobalConfig getGlobalConfigClient() {
            return GlobalConfigInstance.globalConfig;
        }

        public synchronized void refresh(Properties properties) {
            this.properties = properties;
        }

        public synchronized String getValue(String key) {
            return properties.getProperty(key);
        }

        public synchronized Properties getProperties() {
            return properties;
        }

        public synchronized boolean isEmpty() {
            return properties.isEmpty();
        }
    }
}
