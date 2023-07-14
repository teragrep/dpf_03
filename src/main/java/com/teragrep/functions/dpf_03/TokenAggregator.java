package com.teragrep.functions.dpf_03;

/*
 * Teragrep Tokenizer DPF-03
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.teragrep.blf_01.tokenizer.Tokenizer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;


public class TokenAggregator extends Aggregator<Row, TokenBuffer, List<String>> implements Serializable {

    private final String column;

    // Constructor used to feed in the column name
    public TokenAggregator(String column) {
        super();
        this.column = column;
    }

    @Override
    public TokenBuffer zero() {
        return new TokenBuffer();
    }

    @Override
    public TokenBuffer reduce(TokenBuffer b, Row a) {
        String input = a.getAs(column).toString();

        for (String i : Tokenizer.tokenize(input))
            b.addKey(i);

        return b;
    }

    @Override
    public TokenBuffer merge(TokenBuffer b1, TokenBuffer b2) {
        b1.mergeMap(b2.getMap());
        return b1;
    }

    @Override
    public List<String> finish(TokenBuffer reduction) {
	    return new ArrayList<>(reduction.getMap().keySet());
    }

    @Override
    public Encoder<TokenBuffer> bufferEncoder() {
        return Encoders.kryo(TokenBuffer.class);
    }

    @Override
    public Encoder outputEncoder() {
	    List<String> stringList = new ArrayList<>();
        return Encoders.kryo(stringList.getClass());
    }
}
