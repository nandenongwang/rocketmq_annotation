package org.apache.rocketmq.remoting.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class Pair<T1, T2> {
    private T1 object1;
    private T2 object2;
}
