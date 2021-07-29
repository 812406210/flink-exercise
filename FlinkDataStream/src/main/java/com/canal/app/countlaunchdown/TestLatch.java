package com.canal.app.countlaunchdown;

import java.util.concurrent.CountDownLatch;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-27 10:13
 */
public class TestLatch {
    /***
     *  the race begin
     *  0 start !
     *  0 arrived !
     *  1 start !
     *  1 arrived !
     *  the race end
     * @param args
     */
    public static void main(String[] args) {
        CountDownLatch begin = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(2);

        for(int i=0; i<2; i++){
            Thread thread = new Thread(new Player(begin,end),String.valueOf(i));
            thread.start();
        }

        try{
            System.out.println("the race begin");
            begin.countDown(); // 减1变0，唤醒begin.await()
            end.await();//await() 方法具有阻塞作用，也就是说主线程在这里暂停
            System.out.println("the race end");
        }catch(Exception e){
            e.printStackTrace();
        }

    }

}


class Player implements Runnable{

    private CountDownLatch begin;

    private CountDownLatch end;

    Player(CountDownLatch begin,CountDownLatch end){
        this.begin = begin;
        this.end = end;
    }

    @Override
    public void run() {

        try {

            System.out.println(Thread.currentThread().getName() + " start !");;
            begin.await();//因为此时已经为0了，所以不阻塞
            System.out.println(Thread.currentThread().getName() + " arrived !");

            end.countDown();//countDown() 并不是直接唤醒线程,当end.getCount()为0时线程会自动唤醒

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}



