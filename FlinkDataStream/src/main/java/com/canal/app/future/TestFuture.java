package com.canal.app.future;

import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-27 10:20
 */
public class TestFuture {
    /***
     *  main thread start,time->1627353543771
     *  Thread[pool-1-thread-1,5,main] start,time->1627353543771
     *  Thread[pool-1-thread-1,5,main] exit,time->1627353545772
     *  run result->1.2
     *  main thread exit,time->1627353545773
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //自定义线程池
//        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("demo-pool-%d").build();
//        ExecutorService pool = new ThreadPoolExecutor(5, 200,
//                0L, TimeUnit.MILLISECONDS,
//                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        //自定义线程池
        ExecutorService executorService= Executors.newSingleThreadExecutor();
        executorService.submit(()->{

        });
        Future<Double> cf = executorService.submit(()->{
            System.out.println(Thread.currentThread()+" start,time->"+System.currentTimeMillis());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            if(false){ // true 可以测试异常情况
                throw new RuntimeException("test");
            }else{
                System.out.println(Thread.currentThread()+" exit,time->"+System.currentTimeMillis());
                return 1.2;
            }
        });
        System.out.println("main thread start,time->"+System.currentTimeMillis());
        //等待子任务执行完成,如果已完成则直接返回结果
        //如果执行任务异常，则get方法会把之前捕获的异常重新抛出
        System.out.println("run result->"+cf.get());
        System.out.println("main thread exit,time->"+System.currentTimeMillis());
        // 需要调用，否则线程不关闭
        executorService.shutdown();
    }
}
