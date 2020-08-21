package com.yangtzelsl.queue;

public class CircleArrayQueueDemo {
    public static void main(String[] args) {

    }
}

class CircleArray {
    private int maxSize; // 数组最大容量

    // front 变量的含义做一个调整： front 就指向队列的第一个元素, 也就是说 arr[front] 就是队列的第一个元素
    // front 的初始值 = 0
    private int front;

    // rear 变量的含义做一个调整：rear 指向队列的最后一个元素的后一个位置. 因为希望空出一个空间做为约定.
    // rear 的初始值 = 0
    private int rear;

    private int[] arr;

    public CircleArray(int arrMaxSize){
        maxSize = arrMaxSize;

    }
}