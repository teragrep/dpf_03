/*
 * Teragrep Tokenizer DPF-03
 * Copyright (C) 2019, 2020, 2021, 2022, 2023  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.functions.dpf_03;

import com.teragrep.blf_01.Token;
import com.teragrep.blf_01.Tokenizer;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.immutable.$colon$colon;
import scala.collection.mutable.WrappedArray;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class TokenizerUDF implements UDF1<String, scala.collection.immutable.List<WrappedArray<Byte>>> {

    private Tokenizer tokenizer = null;

    @Override
    public scala.collection.immutable.List<WrappedArray<Byte>> call(String s) throws Exception {
        if (tokenizer == null) {
            // "lazy" init
            tokenizer = new Tokenizer(32);
        }

        // create empty Scala immutable List
        scala.collection.immutable.List<WrappedArray<Byte>> rv = scala.collection.immutable.List.empty();

        ByteArrayInputStream bais = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
        List<Token> tokens = tokenizer.tokenize(bais);

        for (Token token : tokens) {
            WrappedArray<Byte> tokenBytesWrappedArray = WrappedArray.make(token.bytes);
            // create new Scala immutabe list containing the old list and the new item.
            rv = new $colon$colon<>(tokenBytesWrappedArray, rv);
        }
        return rv;

    }
}
