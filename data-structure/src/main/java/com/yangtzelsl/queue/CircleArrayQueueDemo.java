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
        arr = new int[maxSize];
    }

    // 判断队列是否满
    public boolean isFull() {
        return (rear + 1) % maxSize == front;
    }

    // 判断队列是否空
    public boolean isEmpty(){
        return rear == front;
    }

    // 添加数据到队列
    public void addQueue(int n){
        // 判断队列是否满
        if (isFull()){
            System.out.println("队列已满，不能添加数据~");
            return;
        }
        // 直接加入数据
        arr[rear] = n;
        // 将 rear 后移，这里必须考虑取模
        rear = (rear + 1) % maxSize;
    }

    // 获取队列的数据，出队列
    public int getQueue() {
        // 判断对列是否为空
        if (isEmpty()) {
            // 抛出异常
            throw new RuntimeException("队列为空，不能取数据");
        }
        // 分析出front是指向队列的第一个元素
        // 1.先把front对应的值保留到一个临时变量
        // 2.将front后移
        // 3.将临时保存的变量返回
        int value = arr[front];
        front = (front+1)%maxSize;
        return value;
    }
}