package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.layout.builder.domain.LambdaCostObj;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestPrometheus
{
    @Test
    public void test () throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/pixels_read_lambda_cost.json")));
        String line;
        StringBuilder json = new StringBuilder();
        while ((line = reader.readLine()) != null)
        {
            json.append(line);
        }

        LambdaCostObj lambdaCostObj = JSON.parseObject(json.toString(), LambdaCostObj.class);

        System.out.println(lambdaCostObj.getData().getResult().get(0).getValue().get(1));
    }
}
