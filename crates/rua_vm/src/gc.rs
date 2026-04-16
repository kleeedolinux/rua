use std::collections::BTreeMap;

use rua::ir::FunctionId;

use crate::value::{ObjRef, Value};

#[derive(Debug, Clone)]
pub enum HeapObject {
    List(Vec<Value>),
    Record {
        fields: BTreeMap<String, Value>,
        meta: Option<Value>,
    },
    Closure {
        function: FunctionId,
        captures: Vec<Value>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generation {
    Young,
    Old,
}

#[derive(Debug, Clone)]
struct Cell {
    obj: HeapObject,
    generation: Generation,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GcStats {
    pub live: usize,
    pub collected: usize,
    pub promoted: usize,
    pub was_full: bool,
}

#[derive(Debug, Clone)]
pub struct GcTelemetry {
    pub minor_collections: usize,
    pub full_collections: usize,
    pub total_collected: usize,
    pub total_promoted: usize,
    pub live_objects: usize,
    pub total_gc_work_units: usize,
    pub last_cycle_work_units: usize,
    pub max_cycle_work_units: usize,
}

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub threshold: usize,
    pub min_threshold: usize,
    pub growth_percent: usize,
    pub full_every_minor: usize,
    pub allocator_strategy: AllocatorStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatorStrategy {
    FreeList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcProfile {
    LowLatency,
    Balanced,
    Throughput,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            threshold: 1024,
            min_threshold: 1024,
            growth_percent: 100,
            full_every_minor: 8,
            allocator_strategy: AllocatorStrategy::FreeList,
        }
    }
}

#[derive(Debug, Clone)]
struct IncrementalCycle {
    sweep_old: bool,
    mark_stack: Vec<usize>,
    sweep_index: usize,
    live: usize,
    collected: usize,
    promoted: usize,
}

#[derive(Debug, Clone)]
pub struct Heap {
    objects: Vec<Option<Cell>>,
    marks: Vec<bool>,
    free_list: Vec<usize>,
    allocated_since_gc: usize,
    minor_since_full: usize,
    config: GcConfig,
    telemetry: GcTelemetry,
    cycle: Option<IncrementalCycle>,
}

impl Default for Heap {
    fn default() -> Self {
        Self::new(GcConfig::default())
    }
}

impl Heap {
    pub fn new(config: GcConfig) -> Self {
        Self {
            objects: Vec::new(),
            marks: Vec::new(),
            free_list: Vec::new(),
            allocated_since_gc: 0,
            minor_since_full: 0,
            config,
            telemetry: GcTelemetry {
                minor_collections: 0,
                full_collections: 0,
                total_collected: 0,
                total_promoted: 0,
                live_objects: 0,
                total_gc_work_units: 0,
                last_cycle_work_units: 0,
                max_cycle_work_units: 0,
            },
            cycle: None,
        }
    }

    pub fn set_threshold(&mut self, threshold: usize) {
        self.config.threshold = threshold.max(self.config.min_threshold);
    }

    pub fn set_full_every_minor(&mut self, count: usize) {
        self.config.full_every_minor = count.max(1);
    }

    pub fn telemetry(&self) -> &GcTelemetry {
        &self.telemetry
    }

    pub fn set_profile(&mut self, profile: GcProfile) {
        match profile {
            GcProfile::LowLatency => {
                self.config.threshold = 512;
                self.config.min_threshold = 512;
                self.config.full_every_minor = 16;
                self.config.growth_percent = 50;
            }
            GcProfile::Balanced => {
                self.config.threshold = 1024;
                self.config.min_threshold = 1024;
                self.config.full_every_minor = 8;
                self.config.growth_percent = 100;
            }
            GcProfile::Throughput => {
                self.config.threshold = 4096;
                self.config.min_threshold = 4096;
                self.config.full_every_minor = 4;
                self.config.growth_percent = 200;
            }
        }
    }

    pub fn live_objects(&self) -> usize {
        self.objects.iter().filter(|slot| slot.is_some()).count()
    }

    pub fn alloc_list(&mut self, items: Vec<Value>) -> Value {
        Value::List(self.alloc(HeapObject::List(items)))
    }

    pub fn alloc_record(&mut self, fields: BTreeMap<String, Value>, meta: Option<Value>) -> Value {
        Value::Record(self.alloc(HeapObject::Record { fields, meta }))
    }

    pub fn alloc_closure(&mut self, function: FunctionId, captures: Vec<Value>) -> Value {
        Value::Closure(self.alloc(HeapObject::Closure { function, captures }))
    }

    pub fn get(&self, id: ObjRef) -> Option<&HeapObject> {
        self.objects
            .get(id.0 as usize)
            .and_then(|slot| slot.as_ref().map(|c| &c.obj))
    }

    pub fn maybe_collect<'a>(
        &mut self,
        roots: impl IntoIterator<Item = &'a Value>,
        budget: usize,
    ) -> Option<GcStats> {
        if self.cycle.is_none() {
            if self.allocated_since_gc < self.config.threshold {
                return None;
            }
            let sweep_old = if self.minor_since_full >= self.config.full_every_minor {
                self.minor_since_full = 0;
                true
            } else {
                self.minor_since_full += 1;
                false
            };
            self.start_cycle(roots, sweep_old);
            return self.incremental_slice(budget);
        }

        self.incremental_slice(budget)
    }

    pub fn collect_full<'a>(&mut self, roots: impl IntoIterator<Item = &'a Value>) -> GcStats {
        self.start_cycle(roots, true);
        loop {
            if let Some(stats) = self.incremental_slice(usize::MAX / 8) {
                return stats;
            }
        }
    }

    fn start_cycle<'a>(&mut self, roots: impl IntoIterator<Item = &'a Value>, sweep_old: bool) {
        for mark in &mut self.marks {
            *mark = false;
        }

        let mut mark_stack = Vec::new();
        for root in roots {
            Self::push_value_objref(root, &mut mark_stack);
        }

        self.cycle = Some(IncrementalCycle {
            sweep_old,
            mark_stack,
            sweep_index: 0,
            live: 0,
            collected: 0,
            promoted: 0,
        });
    }

    fn incremental_slice(&mut self, budget: usize) -> Option<GcStats> {
        let mut work = 0usize;

        while work < budget {
            let Some(cycle) = self.cycle.as_mut() else {
                return None;
            };

            if let Some(idx) = cycle.mark_stack.pop() {
                work += 1;
                if idx >= self.objects.len() || self.marks[idx] {
                    continue;
                }
                self.marks[idx] = true;

                if let Some(cell) = &self.objects[idx] {
                    match &cell.obj {
                        HeapObject::List(items) => {
                            for item in items.iter() {
                                Self::push_value_objref(item, &mut cycle.mark_stack);
                            }
                        }
                        HeapObject::Record { fields, meta } => {
                            for v in fields.values() {
                                Self::push_value_objref(v, &mut cycle.mark_stack);
                            }
                            if let Some(v) = meta {
                                Self::push_value_objref(v, &mut cycle.mark_stack);
                            }
                        }
                        HeapObject::Closure { captures, .. } => {
                            for v in captures.iter() {
                                Self::push_value_objref(v, &mut cycle.mark_stack);
                            }
                        }
                    }
                }
                continue;
            }

            if cycle.sweep_index >= self.objects.len() {
                let stats = self.finish_cycle();
                return Some(stats);
            }

            let idx = cycle.sweep_index;
            cycle.sweep_index += 1;
            work += 1;

            let Some(cell) = &mut self.objects[idx] else {
                continue;
            };

            if self.marks[idx] {
                cycle.live += 1;
                if cell.generation == Generation::Young {
                    cell.generation = Generation::Old;
                    cycle.promoted += 1;
                }
                continue;
            }

            let sweep_this = cycle.sweep_old || cell.generation == Generation::Young;
            if sweep_this {
                self.objects[idx] = None;
                self.free_list.push(idx);
                cycle.collected += 1;
            } else {
                cycle.live += 1;
            }
        }

        None
    }

    fn finish_cycle(&mut self) -> GcStats {
        let cycle = self.cycle.take().expect("finish_cycle without active cycle");
        let work = cycle.live + cycle.collected;

        self.allocated_since_gc = 0;
        self.telemetry.total_collected += cycle.collected;
        self.telemetry.total_promoted += cycle.promoted;
        self.telemetry.live_objects = cycle.live;
        self.telemetry.total_gc_work_units += work;
        self.telemetry.last_cycle_work_units = work;
        self.telemetry.max_cycle_work_units = self.telemetry.max_cycle_work_units.max(work);
        if cycle.sweep_old {
            self.telemetry.full_collections += 1;
        } else {
            self.telemetry.minor_collections += 1;
        }

        self.recompute_threshold(cycle.live);

        GcStats {
            live: cycle.live,
            collected: cycle.collected,
            promoted: cycle.promoted,
            was_full: cycle.sweep_old,
        }
    }

    fn alloc(&mut self, obj: HeapObject) -> ObjRef {
        self.allocated_since_gc += 1;
        let reuse_freelist = matches!(self.config.allocator_strategy, AllocatorStrategy::FreeList);
        if reuse_freelist && let Some(idx) = self.free_list.pop() {
            self.objects[idx] = Some(Cell {
                obj,
                generation: Generation::Young,
            });
            self.marks[idx] = false;
            return ObjRef(idx as u32);
        }

        let idx = self.objects.len();
        self.objects.push(Some(Cell {
            obj,
            generation: Generation::Young,
        }));
        self.marks.push(false);
        ObjRef(idx as u32)
    }

    fn recompute_threshold(&mut self, live: usize) {
        let grown = live.saturating_mul(100 + self.config.growth_percent) / 100;
        self.config.threshold = grown.max(self.config.min_threshold);
    }

    fn push_value_objref(value: &Value, out: &mut Vec<usize>) {
        match value {
            Value::List(id) | Value::Record(id) | Value::Closure(id) => out.push(id.0 as usize),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_unreachable_objects() {
        let mut heap = Heap::new(GcConfig {
            threshold: 1,
            ..Default::default()
        });
        let live = heap.alloc_list(vec![Value::Integer(1)]);
        let _dead = heap.alloc_list(vec![Value::Integer(2)]);
        let stats = heap.collect_full([&live]);
        assert!(stats.collected >= 1);
        assert!(stats.live >= 1);
    }

    #[test]
    fn incremental_finishes_with_small_budget() {
        let mut heap = Heap::new(GcConfig {
            threshold: 1,
            min_threshold: 1,
            ..Default::default()
        });
        let live = heap.alloc_list(vec![Value::Integer(1)]);
        let _dead = heap.alloc_list(vec![Value::Integer(2)]);

        let mut done = None;
        for _ in 0..100 {
            done = heap.maybe_collect([&live], 1);
            if done.is_some() {
                break;
            }
        }

        assert!(done.is_some());
    }
}
