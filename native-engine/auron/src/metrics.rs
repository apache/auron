// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, sync::Arc};

use auron_jni_bridge::{jni_call, jni_new_string};
use datafusion::{common::Result, physical_plan::ExecutionPlan};
use jni::objects::JObject;

/// Last-published absolute metric values, keyed by plan-node path + metric name
/// (for example `output_rows` at the root or `0.1.elapsed_compute`).
pub type MetricSnapshot = HashMap<String, i64>;

pub fn update_metric_node(
    metric_node: JObject,
    execution_plan: Arc<dyn ExecutionPlan>,
    snapshot: &mut MetricSnapshot,
) -> Result<()> {
    update_metric_node_at(metric_node, execution_plan, snapshot, "")
}

fn update_metric_node_at(
    metric_node: JObject,
    execution_plan: Arc<dyn ExecutionPlan>,
    snapshot: &mut MetricSnapshot,
    node_path: &str,
) -> Result<()> {
    if metric_node.is_null() {
        return Ok(());
    }

    // Bind MetricsSet so metric name &str values outlive this call.
    let metrics_set = execution_plan.metrics().unwrap_or_default();
    let metric_values: Vec<(&str, i64)> = metrics_set
        .iter()
        .map(|m| m.value())
        .map(|m| (m.name(), m.as_usize() as i64))
        .collect();
    let deltas = compute_positive_deltas(snapshot, node_path, &metric_values);
    update_metrics(metric_node, &deltas)?;

    for (i, &child_plan) in execution_plan.children().iter().enumerate() {
        let child_metric_node = jni_call!(
            MetricNode(metric_node).getChild(i as i32) -> JObject
        )?;
        update_metric_node_at(
            child_metric_node.as_obj(),
            child_plan.clone(),
            snapshot,
            &child_node_path(node_path, i),
        )?;
    }
    Ok(())
}

fn update_metrics(metric_node: JObject, metric_values: &[(String, i64)]) -> Result<()> {
    for (name, value) in metric_values {
        let jname = jni_new_string!(name)?;
        jni_call!(MetricNode(metric_node).add(jname.as_obj(), *value) -> ())?;
    }
    Ok(())
}

fn snapshot_key(node_path: &str, name: &str) -> String {
    if node_path.is_empty() {
        name.to_string()
    } else {
        format!("{node_path}.{name}")
    }
}

fn child_node_path(node_path: &str, child_index: usize) -> String {
    if node_path.is_empty() {
        child_index.to_string()
    } else {
        format!("{node_path}.{child_index}")
    }
}

/// Aggregates same-name metrics on one node, then returns JNI deltas for values
/// that increased since the last publish. Non-positive deltas are skipped and
/// do not update the snapshot (so a later rebound is not over-counted).
fn compute_positive_deltas(
    snapshot: &mut MetricSnapshot,
    node_path: &str,
    metric_values: &[(&str, i64)],
) -> Vec<(String, i64)> {
    let mut current_by_name: HashMap<&str, i64> = HashMap::new();
    for &(name, value) in metric_values {
        *current_by_name.entry(name).or_insert(0) += value;
    }

    let mut deltas = Vec::new();
    for (name, current) in current_by_name {
        let key = snapshot_key(node_path, name);
        let last = snapshot.get(&key).copied().unwrap_or(0);
        let delta = current - last;
        if delta > 0 {
            snapshot.insert(key, current);
            deltas.push((name.to_string(), delta));
        }
    }
    deltas
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(mut deltas: Vec<(String, i64)>) -> Vec<(String, i64)> {
        deltas.sort_by(|a, b| a.0.cmp(&b.0));
        deltas
    }

    #[test]
    fn first_publish_sends_absolute_as_delta_from_zero() {
        let mut snapshot = MetricSnapshot::new();
        let deltas = compute_positive_deltas(
            &mut snapshot,
            "",
            &[("output_rows", 10), ("elapsed_compute", 3)],
        );
        assert_eq!(
            sorted(deltas),
            vec![
                ("elapsed_compute".to_string(), 3),
                ("output_rows".to_string(), 10),
            ]
        );
        assert_eq!(snapshot.get("output_rows"), Some(&10));
        assert_eq!(snapshot.get("elapsed_compute"), Some(&3));
    }

    #[test]
    fn second_publish_sends_only_increase() {
        let mut snapshot = MetricSnapshot::new();
        let _ = compute_positive_deltas(&mut snapshot, "", &[("output_rows", 10)]);
        let deltas = compute_positive_deltas(&mut snapshot, "", &[("output_rows", 15)]);
        assert_eq!(deltas, vec![("output_rows".to_string(), 5)]);
        assert_eq!(snapshot.get("output_rows"), Some(&15));
    }

    #[test]
    fn unchanged_or_non_positive_is_skipped() {
        let mut snapshot = MetricSnapshot::new();
        let _ = compute_positive_deltas(&mut snapshot, "", &[("output_rows", 10)]);
        let deltas = compute_positive_deltas(
            &mut snapshot,
            "",
            &[("output_rows", 10), ("spilled", 0), ("gauge", -1)],
        );
        assert!(deltas.is_empty());
        assert_eq!(snapshot.get("output_rows"), Some(&10));
        assert!(!snapshot.contains_key("spilled"));
        assert!(!snapshot.contains_key("gauge"));
    }

    #[test]
    fn gauge_dip_does_not_change_last_published() {
        let mut snapshot = MetricSnapshot::new();
        let _ = compute_positive_deltas(&mut snapshot, "", &[("mem", 100)]);
        let dip = compute_positive_deltas(&mut snapshot, "", &[("mem", 40)]);
        assert!(dip.is_empty());
        assert_eq!(snapshot.get("mem"), Some(&100));
        let rebound = compute_positive_deltas(&mut snapshot, "", &[("mem", 120)]);
        assert_eq!(rebound, vec![("mem".to_string(), 20)]);
        assert_eq!(snapshot.get("mem"), Some(&120));
    }

    #[test]
    fn same_name_on_one_node_is_summed() {
        let mut snapshot = MetricSnapshot::new();
        let deltas =
            compute_positive_deltas(&mut snapshot, "", &[("output_rows", 4), ("output_rows", 6)]);
        assert_eq!(deltas, vec![("output_rows".to_string(), 10)]);
        let again =
            compute_positive_deltas(&mut snapshot, "", &[("output_rows", 4), ("output_rows", 7)]);
        assert_eq!(again, vec![("output_rows".to_string(), 1)]);
    }

    #[test]
    fn node_paths_are_independent() {
        let mut snapshot = MetricSnapshot::new();
        let root = compute_positive_deltas(&mut snapshot, "", &[("output_rows", 10)]);
        let child = compute_positive_deltas(&mut snapshot, "0", &[("output_rows", 3)]);
        assert_eq!(root, vec![("output_rows".to_string(), 10)]);
        assert_eq!(child, vec![("output_rows".to_string(), 3)]);
        assert_eq!(snapshot.get("output_rows"), Some(&10));
        assert_eq!(snapshot.get("0.output_rows"), Some(&3));
    }

    #[test]
    fn snapshot_key_and_child_path() {
        assert_eq!(snapshot_key("", "output_rows"), "output_rows");
        assert_eq!(snapshot_key("0.1", "output_rows"), "0.1.output_rows");
        assert_eq!(child_node_path("", 0), "0");
        assert_eq!(child_node_path("0", 1), "0.1");
    }
}
