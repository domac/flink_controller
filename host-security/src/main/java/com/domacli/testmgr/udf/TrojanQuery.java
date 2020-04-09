package com.domacli.testmgr.udf;

import com.alibaba.fastjson.JSON;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;

public class TrojanQuery extends ScalarFunction {

    private HashMap<String, String> results;

    public TrojanQuery() {
        //引入缓存
        results = new HashMap<String, String>();
    }

    public String eval(String md5) {

        if (results.containsKey(md5)) {
            System.out.println("hit from cache");
            return results.get(md5);
        }

        HttpClient httpClient = MyHttpClient.getInstance();
        httpClient.getParams().setSoTimeout(200);
        PostMethod postMethod = new PostMethod("http://localhost:38080/cmd.cgi?cmd=3164");
        postMethod.setRequestHeader("Content-Seq", "1");
        postMethod.setRequestHeader("Content-Mid", "2D8B61F1A85FBB113C3616C34377711D5B2F96AD");
        try {
            String JSON_STRING = "{\"Md5List\":[{\"Md5\":\"" + md5 + "\"}]}";
            StringRequestEntity requestEntity = new StringRequestEntity(
                    JSON_STRING,
                    "application/json",
                    "UTF-8");

            postMethod.setRequestEntity(requestEntity);

            int code = httpClient.executeMethod(postMethod);
            if (code == 200) {
                String text = postMethod.getResponseBodyAsString();

                Result result = JSON.parseObject(text, Result.class);
                if (result == null) {
                    return "-1";
                }

                AckMd5List[] ackMd5List = result.getAckMd5List();
                if (ackMd5List == null || ackMd5List.length == 0) {
                    return "-2";
                }

                int resCode = ackMd5List[0].getSafeLevel();
                String resName = ackMd5List[0].getVirusName();
                String resData = resCode + "#" + resName;
                results.put(md5, resData);
                return resData;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "-3";
    }
}
