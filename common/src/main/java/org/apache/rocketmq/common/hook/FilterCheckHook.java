package org.apache.rocketmq.common.hook;

import java.nio.ByteBuffer;

/**
 * 未使用
 */
@Deprecated
public interface FilterCheckHook {
    public String hookName();

    public boolean isFilterMatched(final boolean isUnitMode, final ByteBuffer byteBuffer);
}
