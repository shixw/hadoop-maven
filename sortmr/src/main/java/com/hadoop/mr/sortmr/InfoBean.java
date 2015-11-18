package com.hadoop.mr.sortmr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xianwei on 2015/8/16.
 */
public class InfoBean implements WritableComparable<InfoBean> {
    private String account;
    private double income;
    private double expenses;
    private double surples;

    public void set(String account,double income,double expenses){
        this.account = account;
        this.income = income;
        this.expenses = expenses;
        this.surples = income - expenses;
    }
    //返回 1  或  -1
    @Override
    public int compareTo(InfoBean o) {
        if (this.income == o.getIncome()) {
            return this.expenses > o.getExpenses() ? 1 : -1;
        } else {
            return this.income > o.getIncome() ? -1 : 1;
        }
    }

    @Override
    public String toString() {
        return this.income + "\t" + this.expenses + "\t" + this.surples;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(account);
        out.writeDouble(income);
        out.writeDouble(expenses);
        out.writeDouble(surples);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.account = in.readUTF();
        this.income = in.readDouble();
        this.expenses = in.readDouble();
        this.surples = in.readDouble();
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public double getSurples() {
        return surples;
    }

    public void setSurples(double surples) {
        this.surples = surples;
    }

    public double getExpenses() {
        return expenses;
    }

    public void setExpenses(double expenses) {
        this.expenses = expenses;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }
}
