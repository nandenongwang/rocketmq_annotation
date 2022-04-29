package org.apache.rocketmq.common.annotation;

import java.lang.annotation.*;

/**
 * 重要字段标识 【仅做提示】
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})
public @interface ImportantField {
}
