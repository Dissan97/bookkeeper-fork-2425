package org.apache.bookkeeper.conf;

public class MyClass {
    public boolean isFalse(boolean tag){
        return !tag;
    }

    public int sum(int a, int b){
        if (a == b){
            return a + b;
        }
        return -(a + b);
    }
}
