package com.domacli.testmgr.udf;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

class AckMd5List {
    @JsonIgnoreProperties("SafeLevel")
    int SafeLevel;

    @JsonIgnoreProperties("VirusName")
    String VirusName;

    public AckMd5List() {
    }

    public AckMd5List(int safeLevel, String virusName) {
        SafeLevel = safeLevel;
        VirusName = virusName;
    }

    public int getSafeLevel() {
        return SafeLevel;
    }

    public void setSafeLevel(int safeLevel) {
        SafeLevel = safeLevel;
    }

    public String getVirusName() {
        return VirusName;
    }

    public void setVirusName(String virusName) {
        VirusName = virusName;
    }

    @Override
    public String toString() {
        return "AckMd5List{" +
                "SafeLevel=" + SafeLevel +
                ", VirusName='" + VirusName + '\'' +
                '}';
    }
}

public class Result implements Serializable {

    @JsonIgnoreProperties("AckMd5List")
    AckMd5List[] ackMd5List;

    public Result() {
    }

    public Result(AckMd5List[] ackMd5List) {
        this.ackMd5List = ackMd5List;
    }

    public AckMd5List[] getAckMd5List() {
        return ackMd5List;
    }

    public void setAckMd5List(AckMd5List[] ackMd5List) {
        this.ackMd5List = ackMd5List;
    }

    @Override
    public String toString() {
        return "Result{" +
                "ackMd5List=" + Arrays.toString(ackMd5List) +
                '}';
    }
}
