package com.alibaba.alink.io.utils;

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.alibaba.alink.common.AlinkSession.gson;

public class CsvUtil {

    public static String getFileProtocol(String filePath) {
        String protocol = null;
        URL url = null;

        try {
            url = new URL(filePath);
            protocol = url.getProtocol();
        } catch (Exception e) {
            protocol = "";
        }

        return protocol;
    }

    public static byte[] readHttpFile(String filePath) {
        try {
            HttpURLConnection connection;
            URL url = new URL(filePath);
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(60000);
            connection.setRequestMethod("GET");
            connection.connect();
            int contentLength = connection.getContentLength();
            InputStream in = connection.getInputStream();
            byte[] buffer = new byte[contentLength];

            int read;
            int totRead = 0;
            while ((read = in.read(buffer, totRead, contentLength - totRead)) != -1) {
                totRead += read;
            }
            connection.disconnect();

            return buffer;
        } catch (Exception e) {
            throw new RuntimeException("Fail to read from: " + filePath + ", mssg: " +
                    e.getMessage());
        }
    }

    public static String[] getColNames(String schemaStr) {
        String[] fields = schemaStr.split(",");
        String[] colNames = new String[fields.length];
        for (int i = 0; i < colNames.length; i++) {
            String[] kv = fields[i].trim().split("\\s+");
            colNames[i] = kv[0];
        }
        return colNames;
    }

    public static TypeInformation[] getColTypes(String schemaStr) {
        String[] fields = schemaStr.split(",");
        TypeInformation[] colTypes = new TypeInformation[fields.length];
        for (int i = 0; i < colTypes.length; i++) {
            String[] kv = fields[i].trim().split("\\s+");
            colTypes[i] = CsvUtil.stringToType(kv[1]);
        }
        return colTypes;
    }

    public static class CsvFieldParser implements Serializable {
        private String schemaStr;
        private String fieldDelimiter;
        private String charsetName;

        private transient byte[] fieldDelimBytes;
        private transient Charset charset;
        private transient String[] fieldNames = null;
        private transient TypeInformation[] fieldTypes = null;

        private transient FieldParser<?>[] fieldParsers = null;
        private transient Object[] holders = null;
        private transient byte[] bytesBuffer = null;

        public CsvFieldParser(String schemaStr, String fieldDelimiter, String charsetName) {
            this.schemaStr = schemaStr;
            this.fieldDelimiter = fieldDelimiter;
            this.charsetName = charsetName;
            init();
        }

        private static Class<?>[] extractTypeClasses(TypeInformation[] fieldTypes) {
            Class<?>[] classes = new Class<?>[fieldTypes.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                classes[i] = fieldTypes[i].getTypeClass();
            }
            return classes;
        }

        public int getNumFields() {
            return fieldNames.length;
        }

        public void init() {
            this.fieldDelimBytes = fieldDelimiter.getBytes(Charset.forName(charsetName));
            this.charset = Charset.forName(charsetName);
            this.fieldNames = CsvUtil.getColNames(schemaStr);
            this.fieldTypes = CsvUtil.getColTypes(schemaStr);
            this.fieldDelimBytes = fieldDelimiter.getBytes(charset);
            this.bytesBuffer = new byte[8 * 1024 * 1024];

            Class<?>[] fieldClasses = extractTypeClasses(fieldTypes);

            // instantiate the parsers
            FieldParser<?>[] parsers = new FieldParser<?>[fieldClasses.length];

            for (int i = 0; i < fieldClasses.length; i++) {
                if (fieldClasses[i] != null) {
                    Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldClasses[i]);
                    if (parserType == null) {
                        throw new RuntimeException("No parser available for type '" + fieldClasses[i].getName() + "'.");
                    }

                    FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);
                    p.setCharset(charset);
                    parsers[i] = p;
                }
            }
            this.fieldParsers = parsers;
            this.holders = new Object[fieldTypes.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                holders[i] = fieldParsers[i].createValue();
            }
        }

        private boolean isFieldDelimOrEnd(byte[] bytes, int pos, int end) {
            if (pos >= end)
                return true;
            if (pos + fieldDelimBytes.length > end)
                return false;

            for (int i = 0; i < fieldDelimBytes.length; i++) {
                if (bytes[pos + i] != fieldDelimBytes[i])
                    return false;
            }
            return true;
        }

        // Flink's parser can't parse numeric data when there are extra space exist.
        // So I have to trim the space manually.
        private int trimSpace(final byte[] bytes, final int offset, final int numBytes) {
            boolean hasNonString = false;
            for (int i = 0; i < fieldTypes.length; i++) {
                if (fieldTypes[i].equals(Types.STRING()))
                    continue;
                hasNonString = true;
                break;
            }

            if (!hasNonString)
                return -1;

            int pos = 0;
            int len = 0;

            for (int i = 0; i < fieldTypes.length; i++) {
                if (fieldTypes[i].equals(Types.STRING())) {// move to the start pos of next field
                    while (!isFieldDelimOrEnd(bytes, offset + pos, offset + numBytes)) {
                        bytesBuffer[len] = bytes[offset + pos];
                        pos++;
                        len++;
                    }
                } else {
                    while (!isFieldDelimOrEnd(bytes, offset + pos, offset + numBytes)) {
                        if (bytes[offset + pos] == ' ' || bytes[offset + pos] == '\t') {
                            pos++;
                            continue;
                        } else {
                            bytesBuffer[len] = bytes[offset + pos];
                            pos++;
                            len++;
                        }
                    }
                }

                for (int j = 0; j < fieldDelimBytes.length; j++) {
                    if (offset + pos >= offset + numBytes)
                        break;
                    bytesBuffer[len] = bytes[offset + pos];
                    pos++;
                    len++;
                }
            }

            return len;
        }

        public Row readRecord(Row reuse, byte[] bytes, int offset, int numBytes) throws IOException {
            Row reuseRow;
            if (reuse == null)
                reuseRow = new Row(fieldTypes.length);
            else
                reuseRow = reuse;

            // Found window's end line, so find carriage return before the newline
            if (numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
                //reduce the number of bytes so that the Carriage return is not taken as data
                numBytes--;
            }

            byte[] trimedBytes;

            if (this.fieldDelimiter.contains(" ") || this.fieldDelimiter.contains("\t")) {
                trimedBytes = bytes;
            } else {
                int len = trimSpace(bytes, offset, numBytes);
                if (len >= 0) {
                    trimedBytes = bytesBuffer;
                    offset = 0;
                    numBytes = len;
                } else {
                    trimedBytes = bytes;
                }
            }

            int startPos = offset;
            for (int field = 0; field < fieldTypes.length; field++) {
                FieldParser<Object> parser = (FieldParser<Object>) fieldParsers[field];

                if (startPos >= offset + numBytes) {
                    reuseRow.setField(field, null);
                    continue;
                }

                int newStartPos = parser.resetErrorStateAndParse(
                        trimedBytes,
                        startPos,
                        offset + numBytes,
                        fieldDelimBytes,
                        holders[field]);

                if (parser.getErrorState() != FieldParser.ParseErrorState.NONE) {
                    if (parser.getErrorState() == FieldParser.ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR) {
                        reuseRow.setField(field, null);
                    } else if (parser.getErrorState() == FieldParser.ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER) {
                        reuseRow.setField(field, null);
                    } else if (parser.getErrorState() == FieldParser.ParseErrorState.EMPTY_COLUMN) {
                        reuseRow.setField(field, null);
                    } else {
                        throw new ParseException(String.format("Parsing error for column %1$s of row '%2$s' originated by %3$s: %4$s.",
                                field, new String(bytes, offset, numBytes), parser.getClass().getSimpleName(), parser.getErrorState()));
                    }
                } else {
                    reuseRow.setField(field, parser.getLastResult());
                }

                if (newStartPos < 0) {
                    if (field < fieldTypes.length - 1) { // skip next field delimiter
                        while (startPos + fieldDelimBytes.length <= offset + numBytes && (!FieldParser.delimiterNext(bytes, startPos, fieldDelimBytes)))
                            startPos++;
                        if (startPos + fieldDelimBytes.length > offset + numBytes)
                            throw new RuntimeException("Can't find next field delimiter: " + "\"" + fieldDelimiter + "\", " +
                                    "Perhaps the data is invalid or do not match the schema." +
                                    "The row is: " + new String(bytes, offset, numBytes));
                        startPos += fieldDelimBytes.length;
                    }
                } else {
                    startPos = newStartPos;
                }
            }

            return reuseRow;
        }

    }

    public static Row convertToRow(String data, Class<?>[] colClasses, String fieldDelim) {
        String[] fields = data.split(fieldDelim);
        if (fields.length != colClasses.length) {
            throw new IllegalArgumentException("Unmatched number of fields: " + fields.length + " vs " + colClasses.length +
                    " data: " + data);
        }
        Row row = new Row(fields.length);
        for (int i = 0; i < fields.length; i++) {
            if (colClasses[i].equals(String.class))
                row.setField(i, fields[i]);
            else
                row.setField(i, gson.fromJson(fields[i], colClasses[i]));
        }
        return row;
    }

    public static TypeInformation stringToType(String type) {
        if (type.compareToIgnoreCase("boolean") == 0) {
            return Types.BOOLEAN();
        } else if (type.compareToIgnoreCase("datetime") == 0) {
            return Types.STRING();
        } else if (type.compareToIgnoreCase("long") == 0) {
            return Types.LONG();
        } else if (type.compareToIgnoreCase("bigint") == 0) {
            return Types.LONG();
        } else if (type.compareToIgnoreCase("int") == 0) {
            return Types.INT();
        } else if (type.compareToIgnoreCase("double") == 0) {
            return Types.DOUBLE();
        } else if (type.compareToIgnoreCase("float") == 0) {
            return Types.FLOAT();
        } else if (type.compareToIgnoreCase("string") == 0) {
            return Types.STRING();
        } else {
            throw new RuntimeException("Not supported data type: " + type);
        }
    }

    public static String typeToString(TypeInformation type) {
        if (type.equals(Types.STRING())) {
            return "string";
        } else if (type.equals(Types.LONG())) {
            return "bigint";
        } else if (type.equals(Types.INT())) {
            return "int";
        } else if (type.equals(Types.DOUBLE())) {
            return "double";
        } else if (type.equals(Types.FLOAT())) {
            return "float";
        } else if (type.equals(Types.BOOLEAN())) {
            return "boolean";
        } else {
            throw new RuntimeException("Not supported data type: " + type.toString());
        }
    }

    public static Class typeToClass(TypeInformation<?> type) {
        if (type.equals(Types.STRING()))
            return String.class;
        else if (type.equals(Types.DOUBLE()))
            return Double.class;
        else if (type.equals(Types.FLOAT()))
            return Float.class;
        else if (type.equals(Types.INT()))
            return Integer.class;
        else if (type.equals(Types.LONG()))
            return Long.class;
        else if (type.equals(Types.BOOLEAN()))
            return Boolean.class;
        else
            throw new RuntimeException("Not supported data type: " + type.toString());
    }

    private static int extractEscape(String s, int pos, StringBuilder sbd) {
        if (s.charAt(pos) != '\\')
            return 0;

        pos++;

        if (pos >= s.length())
            return 0;

        char c = s.charAt(pos);

        if (c >= '0' && c <= '7') {
            int digit = 1;
            int i;
            for (i = 0; i < 2; i++) {
                if (pos + 1 + i >= s.length())
                    break;
                if (s.charAt(pos + 1 + i) >= '0' && s.charAt(pos + 1 + i) <= '7')
                    digit++;
                else
                    break;
            }
            int n = Integer.valueOf(s.substring(pos, pos + digit), 8);
            sbd.append(Character.toChars(n));
            return digit + 1;
        } else if (c == 'u') { // unicode
            pos++;
            int digit = 0;
            for (int i = 0; i < 4; i++) {
                if (pos + i >= s.length())
                    break;
                char ch = s.charAt(pos + i);
                if ((ch >= '0' && ch <= '9') || ((ch >= 'a' && ch <= 'f')) || ((ch >= 'A' && ch <= 'F')))
                    digit++;
                else
                    break;
            }
            if (digit == 0)
                return 0;
            int n = Integer.valueOf(s.substring(pos, pos + digit), 16);
            sbd.append(Character.toChars(n));
            return digit + 2;
        } else {
            switch (c) {
                case '\\':
                    sbd.append('\\');
                    return 2;
                case '\'':
                    sbd.append('\'');
                    return 2;
                case '\"':
                    sbd.append('"');
                    return 2;
                case 'r':
                    sbd.append('\r');
                    return 2;
                case 'f':
                    sbd.append('\f');
                    return 2;
                case 't':
                    sbd.append('\t');
                    return 2;
                case 'n':
                    sbd.append('\n');
                    return 2;
                case 'b':
                    sbd.append('\b');
                    return 2;
                default:
                    return 0;
            }
        }
    }


    /**
     * 从Java字面字符解析出其中的转义字符和unicode字符。
     * <p>
     * 例如，
     * 字面字符串 s = "\\t" 将被解析为 e = "\t"，表示制表符
     * 字面字符串 s = "\\001" 将被解析为 e = "\001"，表示'ctrl A'
     * 字面字符串 s = "\\u0001" 将被解析为 e = "\u0001"，表示'ctrl A'
     *
     * @param s，待解析的字符串
     * @return 注：被解析的转义字符包括: \b, \f, \n, \r, \t, \\, \', \", \ddd (1到3位八进制数所代表的任意字符),
     * \udddd (1到4位十六进制数所表示的unicode字符)
     */

    public static String unEscape(String s) {
        if (s == null)
            return null;

        if (s.length() == 0)
            return s;

        StringBuilder sbd = new StringBuilder();

        for (int i = 0; i < s.length(); ) {
            int flag = extractEscape(s, i, sbd);
            if (flag <= 0) {
                sbd.append(s.charAt(i));
                i++;
            } else {
                i += flag;
            }
        }

        return sbd.toString();
    }

    public static void unzipFile(String fn, String dir) {
        //Open the file
        try (ZipFile file = new ZipFile(fn)) {
            FileSystem fileSystem = FileSystems.getDefault();
            //Get file entries
            Enumeration<? extends ZipEntry> entries = file.entries();

            //We will unzip files in this folder
            String uncompressedDirectory = dir + File.separator;
            Files.createDirectory(fileSystem.getPath(uncompressedDirectory));

            //Iterate over entries
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                //If directory then create a new directory in uncompressed folder
                if (entry.isDirectory()) {
                    System.out.println("Creating Directory:" + uncompressedDirectory + entry.getName());
                    Files.createDirectories(fileSystem.getPath(uncompressedDirectory + entry.getName()));
                }
                //Else create the file
                else {
                    InputStream is = file.getInputStream(entry);
                    BufferedInputStream bis = new BufferedInputStream(is);
                    String uncompressedFileName = uncompressedDirectory + entry.getName();
                    Path uncompressedFilePath = fileSystem.getPath(uncompressedFileName);
                    Files.createFile(uncompressedFilePath);
                    FileOutputStream fileOutput = new FileOutputStream(uncompressedFileName);
                    while (bis.available() > 0) {
                        fileOutput.write(bis.read());
                    }
                    fileOutput.close();
                    System.out.println("Written :" + entry.getName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String downloadFile(String filePath, String dir) {
        try {
            HttpURLConnection connection;
            URL url = new URL(filePath);
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(60000);
            connection.setRequestMethod("GET");
            connection.connect();

            int indexOf = filePath.lastIndexOf("/");
            String fn = dir + File.separator + filePath.substring(indexOf + 1);

            int read;
            final int buffSize = 64 * 1024;
            byte[] buffer = new byte[buffSize];
            InputStream in = connection.getInputStream();

            FileOutputStream fos = new FileOutputStream(fn);

            while ((read = in.read(buffer, 0, buffSize)) != -1) {
                fos.write(buffer, 0, read);
            }

            connection.disconnect();
            fos.close();

            return fn;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Fail to download file " + filePath);
        }
    }

    public static String createTempDirectory(String prefix) throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        File generatedDir = new File(tempDir, prefix + System.nanoTime());

        if (!generatedDir.mkdir())
            throw new IOException("Failed to create temp directory " + generatedDir.getName());

        return generatedDir.getPath();
    }
}
