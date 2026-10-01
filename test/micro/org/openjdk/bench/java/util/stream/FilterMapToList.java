/*
 * Copyright (c) 2026, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package org.openjdk.bench.java.util.stream;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.*;
import org.openjdk.jmh.annotations.*;

/**
 * Compares short sequential pipelines over an {@code ArrayList} that end in
 * {@code toList()} with equivalent hand-written loops, in time and (with
 * {@code -prof gc}) in allocated bytes.
 *
 * With {@code polluted=true} the stream implementation is first exercised
 * with many other pipeline shapes, as it would be in any real application.
 * This pollutes the type profiles of the call sites shared by all pipelines,
 * so that the JIT can no longer rely on them to inline the user-supplied
 * functions.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class FilterMapToList {
    public record Person(String name, int age) {}

    @Param({"1", "10", "100", "1000", "10000"})
    int size;

    @Param({"false", "true"})
    boolean polluted;

    List<Person> list;

    @Setup
    public void setup() {
        list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // 50% selectivity, in predictable runs of 18
            list.add(new Person("p" + i, (i + 18) % 36));
        }
        if (polluted) {
            pollute();
        }
    }

    // Exercise the shared stream machinery with many other pipeline shapes so that
    // type profiles in java.util.stream look like they would in a real application.
    static volatile Object sink;

    static void pollute() {
        List<Integer> al = new ArrayList<>(IntStream.range(0, 64).boxed().toList());
        List<Integer> imm = List.copyOf(al);
        List<Integer> asList = Arrays.asList(al.toArray(new Integer[0]));
        Set<Integer> hs = new HashSet<>(al);
        LinkedList<Integer> ll = new LinkedList<>(al);
        for (int i = 0; i < 30_000; i++) {
            sink = al.stream().map(x -> x + 1).filter(x -> (x & 1) == 0).toList();
            sink = al.stream().filter(x -> x > 3).collect(Collectors.toList());
            sink = al.stream().map(String::valueOf).collect(Collectors.joining(","));
            sink = al.stream().mapToInt(Integer::intValue).sum();
            sink = al.stream().sorted(Comparator.reverseOrder()).limit(5).toList();
            sink = al.stream().flatMap(x -> Stream.of(x, x)).distinct().count();
            sink = al.stream().peek(x -> {}).anyMatch(x -> x == 63);
            sink = imm.stream().filter(x -> x % 3 == 0).map(x -> x * 2).toList();
            sink = imm.stream().map(Object::toString).toArray(String[]::new);
            sink = asList.stream().filter(Objects::nonNull).map(x -> -x).toList();
            sink = asList.stream().skip(3).reduce(0, Integer::sum);
            sink = hs.stream().filter(x -> x < 40).map(x -> (long) x).collect(Collectors.toSet());
            sink = ll.stream().map(x -> x ^ 5).filter(x -> x != 7).toList();
            sink = Stream.of("a", "b", "c").map(String::length).filter(x -> x > 0).toList();
            sink = IntStream.range(0, 32).boxed().filter(x -> x > 2).map(x -> "" + x).toList();
            sink = al.stream().collect(Collectors.groupingBy(x -> x % 4, Collectors.counting()));
            al.stream().filter(x -> x > 60).forEach(x -> sink = x);
        }
    }

    @Benchmark
    public List<String> stream() {
        return list.stream().filter(p -> p.age() >= 18).map(Person::name).toList();
    }

    @Benchmark
    public List<String> mapOnlyStream() {
        return list.stream().map(Person::name).toList();
    }

    @Benchmark
    public List<String> mapOnlyLoopPresized() {
        List<String> out = new ArrayList<>(list.size());
        for (Person p : list) {
            out.add(p.name());
        }
        return out;
    }

    // Capturing lambdas: are the lambda objects removed as well?
    @Benchmark
    public List<String> streamCapturing() {
        int min = size >= 0 ? 18 : 0;
        String suffix = "";
        return list.stream().filter(p -> p.age() >= min).map(p -> p.name().concat(suffix)).toList();
    }

    // A longer pipeline, to check that this isn't specific to two stages.
    @Benchmark
    public List<Integer> streamFourStages() {
        return list.stream().filter(p -> p.age() >= 18).map(Person::name).filter(n -> !n.isEmpty()).map(String::length).toList();
    }

    @Benchmark
    public List<Integer> loopFourStages() {
        List<Integer> out = new ArrayList<>();
        for (Person p : list) {
            if (p.age() >= 18) {
                String n = p.name();
                if (!n.isEmpty()) {
                    out.add(n.length());
                }
            }
        }
        return out;
    }

    @Benchmark
    public List<String> streamCollectToList() {
        return list.stream().filter(p -> p.age() >= 18).map(Person::name).collect(Collectors.toList());
    }

    @Benchmark
    public List<String> loopArrayList() {
        List<String> out = new ArrayList<>();
        for (Person p : list) {
            if (p.age() >= 18) {
                out.add(p.name());
            }
        }
        return out;
    }

    @Benchmark
    public List<String> loopArrayListCopyOf() {
        List<String> out = new ArrayList<>();
        for (Person p : list) {
            if (p.age() >= 18) {
                out.add(p.name());
            }
        }
        return List.copyOf(out);
    }

    @Benchmark
    public List<String> loopPresized() {
        List<String> out = new ArrayList<>(list.size());
        for (Person p : list) {
            if (p.age() >= 18) {
                out.add(p.name());
            }
        }
        return out;
    }

    // Allocation lower bound: count first, then allocate exactly once.
    @Benchmark
    public List<String> loopTwoPass() {
        int n = 0;
        for (Person p : list) {
            if (p.age() >= 18) {
                n++;
            }
        }
        String[] out = new String[n];
        int i = 0;
        for (Person p : list) {
            if (p.age() >= 18) {
                out[i++] = p.name();
            }
        }
        return Arrays.asList(out);
    }
}
