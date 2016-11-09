# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This is a basic script that parses the output of fchan_main and renders the
graphs for goroutines=GOMAXPROCS and goroutines=5000
"""
import numpy as np
import matplotlib.pyplot as plt
import seaborn as unused_import
import re
import sys


class BenchResult(object):
    def __init__(self, name, gmp, max_hw_thr, nops, secs):
        self.name = name
        self.gmp = gmp
        self.max_hw_thr = int(max_hw_thr)
        # Millions of operations per second
        self.tp = float(nops) / (float(secs) * 1e6)


def parse_line(line):
    m_gmp = re.match(r'^([^\-]*)GMP-(\d+)\s+(\d+)\s+([^\s]*)s\s*$',
                     line)
    m2 = re.match(r'^([^\-]*)-(\d+)\s+(\d+)\s+([^\s]*)s\s*$', line)
    if m_gmp is not None:
        name, threads, nops, secs = m_gmp.groups()
        return BenchResult(name, True, threads, nops, secs)
    if m2 is not None:
        name, threads, nops, secs = m2.groups()
        return BenchResult(name, False, threads, nops, secs)
    print line, 'did not match anything'
    return None


def plot_points(all_results, gmp):
    series = sorted(list({k.name for k in all_results if k.gmp == gmp}))
    for k in series:
        results = [r for r in all_results if r.gmp == gmp and r.name == k]
        points = sorted((r.max_hw_thr, r.tp) for r in results)
        plt.xlabel(r'GOMAXPROCS')
        plt.ylabel('Ops / second (millions)')
        X = np.array([x for (x, y) in points])
        Y = np.array([y for (x, y) in points])
        plt.plot(X, Y, label=k)
        plt.scatter(X, Y)
        plt.legend()


def main(fname):
    with open(fname) as f:
        results = [p for p in (parse_line(line) for line in f)
                   if p is not None]
        print 'Generating non-GMP graph'
        plt.title('5000 Goroutines')
        plot_points(results, False)
        plt.savefig('contend_graph.pdf')
        plt.clf()
        print 'Generating GMP graph'
        plt.title('Goroutines Equal to GOMAXPROCS')
        plot_points(results, True)
        plt.savefig('gmp_graph.pdf')
        plt.clf()

if __name__ == '__main__':
    main(sys.argv[1])
