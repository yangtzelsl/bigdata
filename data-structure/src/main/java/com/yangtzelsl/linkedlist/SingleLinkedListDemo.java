package com.yangtzelsl.linkedlist;

/**
 * @Author: liusilin
 * @Date: 2020/9/3 22:15
 * @Description:
 */
public class SingleLinkedListDemo {

    public static void main(String[] args) {

    }
}

// 定义SingleLinkedList 管理英雄
class SingleLinkedList {
    // 先初始化一个头结点，头结点不要动，不存放具体的数据
    private HeroNode head = new HeroNode(0, "", "");

    // 添加节点到
}

// 定义HeroNode，每个HeroNode对象就是一个节点
class HeroNode {
    public int no;
    public String name;
    public String nickName;
    public HeroNode next;//指向下一个节点

    // 构造器
    public HeroNode(int no, String name, String nickName) {
        this.no = no;
        this.name = name;
        this.nickName = nickName;
    }

    // 显示方便重写toString

    @Override
    public String toString() {
        return "HeroNode{" +
                "no=" + no +
                ", name='" + name + '\'' +
                ", nickName='" + nickName + '\'' +
                ", next=" + next +
                '}';
    }
}