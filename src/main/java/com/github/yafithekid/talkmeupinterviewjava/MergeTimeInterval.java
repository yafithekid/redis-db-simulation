package com.github.yafithekid.talkmeupinterviewjava;


import io.swagger.models.auth.In;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

class Interval {
    int a;
    int b;

    Interval(int a,int b){
        this.a = a;
        this.b = b;
    }

    static boolean isIntersect(Interval p,Interval q){
        return (q.a <= p.a && p.a <= q.b) || (q.a <= p.b && p.b <= q.b);
    }

    static Interval merge(Interval p,Interval q){
        return new Interval(Math.min(p.a,q.a), Math.max(p.b,q.b));
    }
}

public class MergeTimeInterval {
    public static void main(String[] args){
        int[][] input = new int[][]{{1,3}, {2,4}, {5,7}, {6,8} };
        List<Interval> result = merge(input);
        System.out.println(result);
    }

    public static List<Interval> merge(int[][] input){
//        List<Interval> intervals;

        PriorityQueue<Interval> pq = new PriorityQueue<>(Comparator.comparingInt(o -> o.a));
        for(int i = 0; i < input.length; i++){
            pq.add(new Interval(input[i][0],input[i][1]));
        }

        List<Interval> result = new ArrayList<>();
        while (!pq.isEmpty()){
            Interval top = pq.poll();
            if (!pq.isEmpty()){
                Interval secondTop = pq.peek();
                if (Interval.isIntersect(top,secondTop)){
                    pq.poll();
                    top = Interval.merge(top,secondTop);
                }
            }
            result.add(top);
        }
        return result;

    }
}
