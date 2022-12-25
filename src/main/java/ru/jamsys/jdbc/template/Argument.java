package ru.jamsys.jdbc.template;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Argument {

    List<Integer> index = new ArrayList<>();
    ArgumentDirection direction;
    ArgumentType type;

}
