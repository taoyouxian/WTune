package cn.edu.ruc.iir.rainbow.layout;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestInstanceof
{
    @Test
    public void test ()
    {
        ArrayList<String> list = new ArrayList<>();
        System.out.println(list instanceof List);
    }
}
