package ru.jamsys.jdbc.template;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class CompiledSqlTemplate {
    private List<Argument> listArgument = new ArrayList<>();
    private String sql;
}
