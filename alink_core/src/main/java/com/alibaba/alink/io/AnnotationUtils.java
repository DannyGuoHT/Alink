package com.alibaba.alink.io;

import com.alibaba.alink.common.AlinkParameter;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class AnnotationUtils {

    public static Set<Class<?>> allAnntated(String prefix, Class cls) {
        Reflections reflections = new Reflections(prefix);
        return reflections.getTypesAnnotatedWith(cls);
    }

    public static class Annotations {
        Set<Class<?>> clses;

        public Annotations(String prefix, Class cls) {
            this.clses = allAnntated(prefix, cls);
        }

        public Set<Class<?>> getClses() {
            return clses;
        }
    }

    public static class NameAnnotations extends Annotations {
        public NameAnnotations() {
            super("com.alibaba.alink", AlinkIONameAnnotation.class);
        }
    }

    public static class TypeAnnotations extends Annotations {
        public TypeAnnotations() {
            super("com.alibaba.alink", AlinkIOTypeAnnotation.class);
        }
    }

    public static class NameNotationsSingleton {
        private static class InstanceHolder {
            private static final NameAnnotations INSTANCE = new NameAnnotations();
        }

        private NameNotationsSingleton() {}

        public static final NameAnnotations getInstance() {
            return InstanceHolder.INSTANCE;
        }
    }

    public static class TypeAnnotationsSingleton {
        private static class InstanceHolder {
            private static final TypeAnnotations INSTANCE = new TypeAnnotations();
        }

        private TypeAnnotationsSingleton() {}

        public static final TypeAnnotations getInstance() {
            return InstanceHolder.INSTANCE;
        }
    }

    public static String annotationName(Class<?> cls) {
        AlinkIONameAnnotation notation = cls.getAnnotation(AlinkIONameAnnotation.class);
        if (notation == null) {
            return null;
        }

        return notation.name();
    }

    public static boolean annotationStart(Class<?> cls) {
        AlinkIONameAnnotation annotation = cls.getAnnotation(AlinkIONameAnnotation.class);
        if (annotation == null) {
            return false;
        }

        return annotation.start();
    }

    public static String annotationAlias(Class<?> cls) {
        AlinkDBAnnotation annotation = cls.getAnnotation(AlinkDBAnnotation.class);

        if (annotation == null) {
            return null;
        }

        return annotation.tableNameAlias();
    }

    public static AlinkIOType annotationType(Class<?> cls) {
        AlinkIOTypeAnnotation annotation = cls.getAnnotation(AlinkIOTypeAnnotation.class);

        if (annotation == null) {
            return null;
        }

        return annotation.type();
    }

    public static Set<Class<?>> allName() {
        return NameNotationsSingleton.getInstance().getClses();
    }

    public static Set<Class<?>> allType() {
        return TypeAnnotationsSingleton.getInstance().getClses();
    }

    public static List<Class<?>> pickName(Set<Class<?>> clses, String name) {
        List<Class<?>> ret = new ArrayList<>();
        for (Class<?> cls : clses) {
            if (annotationName(cls).equals(name)) {
                ret.add(cls);
            }
        }

        return ret;
    }

    public static List<Class<?>> pickType(Set<Class<?>> clses, AlinkIOType type) {
        List<Class<?>> ret = new ArrayList<>();
        for (Class<?> cls : clses) {
            if (annotationType(cls).equals(type)) {
                ret.add(cls);
            }
        }

        return ret;
    }


    public static boolean isAlinkDB(String name) {
        List<Class<?>> picked = pickName(allName(), name);

        if (picked.size() != 1) {
            return false;
        }

        for (Class<?> cls : picked) {
            String tableNameAlias = annotationAlias(cls);

            if (tableNameAlias != null) {
                return true;
            }
        }

        return false;
    }

    public static Class<?> dbCLS(String name) throws Exception {
        Set<Class<?>> all = allName();

        List<Class<?>> picked = pickName(all, name);

        if (picked.size() > 1) {
            throw new Exception("Pick db error. cls: " + picked);
        }

        if (picked.isEmpty()) {
            return null;
        }

        return picked.get(0);
    }

    public static Class<?> opCLS(String name, AlinkIOType type) throws Exception {
        Set<Class<?>> allName = allName();
        Set<Class<?>> allType = allType();

        List<Class<?>> pickedNames = pickName(allName, name);
        List<Class<?>> pickedTypes = pickType(allType, type);

        Class<?> ret = null;
        for (Class<?> pickedName : pickedNames) {
            for (Class<?> pickedType : pickedTypes) {
                if (pickedName.equals(pickedType)) {
                    if (ret == null) {
                        ret = pickedType;
                    } else {
                        throw new Exception("Pick name and type error. name: " + name + ", type: " + type);
                    }
                }
            }
        }

        return ret;
    }

    public static AlinkDB createDB(String name, AlinkParameter parameter) throws Exception {
        Class<?> cls = dbCLS(name);

        return (AlinkDB) cls.getConstructor(AlinkParameter.class).newInstance(parameter);
    }

    public static Object createOp(String name, AlinkIOType type, AlinkParameter parameter)
            throws Exception {

        Class<?> c = opCLS(name, type);

        if (c == null) {
            return null;
        }

        return c.getConstructor(AlinkParameter.class).newInstance(parameter);
    }
}
