package com.lpy.sketch;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * / **
 *  *计算字符串的MD5哈希值。
 * * < p />
 */

@Description(name = "hash_md5",
        value = "_FUNC_(x) - Hash MD5. "
)
public class HashMD5UDF extends UDF {

    private HashFunction hash = Hashing.md5();


    public Long evaluate(String str) {
        HashCode hc = hash.hashUnencodedChars(str);

        return hc.asLong();
    }

}
