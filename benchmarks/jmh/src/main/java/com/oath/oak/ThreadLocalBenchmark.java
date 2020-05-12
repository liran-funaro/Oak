/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2, time = 30)
@Measurement(iterations = 3, time = 30)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@Threads(12)
@State(Scope.Benchmark)
public class ThreadLocalBenchmark
{
    interface LocalThreadContext {
        ThreadContext get(long tid);
    }

    static class NewThreadLocal implements LocalThreadContext {
        public ThreadContext get(long tid) {
            return new ThreadContext(0, v);
        }
    }

    static class JavaThreadLocal extends ThreadLocal<ThreadContext> implements LocalThreadContext {
        @Override
        protected ThreadContext initialValue() {
            return new ThreadContext(0, v);
        }

        @Override
        public ThreadContext get(long tid) {
            ThreadContext ret = super.get();
            ret.invalidate();
            return ret;
        }
    }

    static class IndexCalculator implements LocalThreadContext {
        final ThreadIndexCalculator contextThreadIndexCalculator = ThreadIndexCalculator.newInstance();
        final ThreadContext[] threadLocalContext = new ThreadContext[ThreadIndexCalculator.MAX_THREADS];

        protected int getIndex(long tid) {
            return contextThreadIndexCalculator.getIndex();
        }

        public ThreadContext get(long tid) {
            int threadIndex = getIndex(tid);
            ThreadContext ret = threadLocalContext[threadIndex];
            if (ret == null) {
                ret = new ThreadContext(threadIndex, v);
                threadLocalContext[threadIndex] = ret;
            }
            ret.invalidate();
            return ret;
        }
    }

    static class IndexCalculatorRandID extends IndexCalculator {
        @Override
        protected int getIndex(long tid) {
            return contextThreadIndexCalculator.getIndex(tid);
        }
    }

    public static LocalThreadContext getImpl(String impl) {
        switch (impl) {
            case "new":
                return new NewThreadLocal();
            case "java":
                return new JavaThreadLocal();
            case "index-calc":
                return new IndexCalculator();
            case "index-calc-rand-id":
                return new IndexCalculatorRandID();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Param({"new", "java", "index-calc", "index-calc-rand-id"})
    private String impl;

    private LocalThreadContext l;

    public static final ValueUtils v = new ValueUtilsImpl();

    @Setup(Level.Iteration)
    public void setup() {
        l = getImpl(impl);
    }

    @TearDown(Level.Iteration)
    public void close() {
        l = null;
    }

    final private static ThreadLocal<Random> s_random = new ThreadLocal<Random>() {
        @Override
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    @State(Scope.Thread)
    public static class ThreadState {
        public long tid;

        int[] cache = new int[1<<10];

        @Setup(Level.Iteration)
        public void setup() {
            tid = s_random.get().nextInt(1<<16);
        }

        @Setup(Level.Invocation)
        public void cleanCache(Blackhole blackhole) {
            for (int i = 0; i < cache.length; i++) {
                cache[i] = s_random.get().nextInt();
                blackhole.consume(cache[i]);
            }
        }
    }


    @Benchmark
    public void getThreadLocal(Blackhole blackhole, ThreadState s) {
        ThreadContext ctx = l.get(s.tid);
        blackhole.consume(ctx);
    }

    @Benchmark
    public void getThreadLocalAndUse(Blackhole blackhole, ThreadState s) {
        ThreadContext ctx = l.get(s.tid);
        blackhole.consume(ctx.entryIndex);
        blackhole.consume(ctx.key.blockID);
        blackhole.consume(ctx.value.blockID);
        blackhole.consume(ctx.valueState);
        blackhole.consume(ctx.newValue.blockID);
        blackhole.consume(ctx.isNewValueForMove);
        blackhole.consume(ctx.result.operationResult);
        blackhole.consume(ctx.tempKey.blockID);
        blackhole.consume(ctx.tempValue.blockID);
    }

    // /usr/bin/java -jar benchmarks/jmh/target/benchmarks-jmh.jar -si false ThreadLocalBenchmark
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include("ThreadLocalBenchmark")
                .forks(0)
                .build();

        new Runner(opt).run();
    }
}
