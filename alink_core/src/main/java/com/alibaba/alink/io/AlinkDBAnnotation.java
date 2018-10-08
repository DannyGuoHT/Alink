package com.alibaba.alink.io;

import java.lang.annotation.*;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface AlinkDBAnnotation {
    String tableNameAlias();
}
