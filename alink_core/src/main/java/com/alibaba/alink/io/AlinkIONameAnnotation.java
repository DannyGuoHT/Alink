package com.alibaba.alink.io;

import java.lang.annotation.*;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface AlinkIONameAnnotation {
    String name();
    boolean start() default false;
}
