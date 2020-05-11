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

        public ThreadContext get(long tid) {
            int threadIndex = contextThreadIndexCalculator.getIndex();
            ThreadContext ret = threadLocalContext[threadIndex];
            if (ret == null) {
                ret = new ThreadContext(threadIndex, v);
                threadLocalContext[threadIndex] = ret;
            }
            ret.invalidate();
            return ret;
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
            default:
                throw new IllegalArgumentException();
        }
    }

    // /usr/bin/java -jar benchmarks/jmh/target/benchmarks-jmh.jar getThreadLocal
    @Param({"new", "java", "index-calc"})
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

        @Setup(Level.Iteration)
        public void setup() {
            tid = s_random.get().nextInt(1<<16);
        }
    }

    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Threads(12)
    @Benchmark
    public void getThreadLocal(Blackhole blackhole, ThreadState s) {
        ThreadContext ctx = l.get(s.tid);
        blackhole.consume(ctx);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include("getThreadLocal")
                .forks(0)
                .threads(12)
                .build();

        new Runner(opt).run();
    }

}
