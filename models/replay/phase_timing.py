"""Opt-in engineering timing. Wall clock never enters simulated policy time."""
from collections import Counter
from contextlib import contextmanager
import gc
import time


class ReplayPhaseTiming:
    def __init__(self):
        self.phases = Counter()
        self.phase_cpu = Counter()
        self.nested = Counter()
        self.nested_calls = Counter()
        self.nested_first_calls = {}
        self.nested_max = Counter()
        self.gc = Counter()
        self.current = None
        self.wall = self.cpu = 0
        self._gc_start = {}

    def mark(self, name):
        wall, cpu = time.perf_counter(), time.process_time()
        if self.current is not None:
            self.phases[self.current] += wall-self.wall
            self.phase_cpu[self.current] += cpu-self.cpu
        self.current, self.wall, self.cpu = name, wall, cpu

    def gc_callback(self, phase, info):
        generation = info['generation']
        if phase == 'start':
            self._gc_start[generation] = time.perf_counter()
        elif generation in self._gc_start:
            self.gc[f'generation_{generation}_seconds'] += time.perf_counter()-self._gc_start.pop(generation)
            self.gc[f'generation_{generation}_calls'] += 1

    @contextmanager
    def collect_gc(self):
        gc.callbacks.append(self.gc_callback)
        try:
            yield
        finally:
            gc.callbacks.remove(self.gc_callback)

    def wrap(self, name, function):
        def measured(*args, **kwargs):
            started = time.perf_counter()
            try:
                return function(*args, **kwargs)
            finally:
                elapsed = time.perf_counter()-started
                self.nested[name] += elapsed
                self.nested_calls[name] += 1
                self.nested_max[name] = max(self.nested_max[name], elapsed)
                first = self.nested_first_calls.setdefault(name, [])
                if len(first) < 3:
                    first.append(elapsed)
        return measured

    @contextmanager
    def phase(self, name):
        """External call-stack timing; never inserted into replay parameters."""
        previous = self.current
        self.mark(name)
        try:
            yield
        finally:
            self.mark(previous)

    def report(self):
        return dict(exclusive_wall=dict(self.phases), exclusive_cpu=dict(self.phase_cpu),
                    nested_wall_do_not_sum=dict(self.nested), nested_calls=dict(self.nested_calls),
                    nested_first_calls=self.nested_first_calls, nested_max=dict(self.nested_max),
                    gc_overlap_do_not_add=dict(self.gc))

    @contextmanager
    def measure_methods(self, methods):
        originals = []
        try:
            for owner, attr, label in methods:
                original = getattr(owner, attr)
                originals.append((owner, attr, original))
                setattr(owner, attr, self.wrap(label, original))
            with self.collect_gc():
                yield
        finally:
            for owner, attr, original in reversed(originals):
                setattr(owner, attr, original)
