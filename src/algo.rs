pub mod algo {
    use crate::graph::graph::*;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::f64::INFINITY;
    extern crate rayon;
    use rayon::prelude::*;

    fn min_dist_node(queue: &HashSet<Node>, dist: &HashMap<String, f64>) -> String {
        // minimum distance node
        queue.iter().fold("".to_string(), |mut min_node, node| {
            let mut current_min_dist = INFINITY;
            if dist.contains_key(&min_node) {
                current_min_dist = dist[&min_node];
            }
            if dist[&node.id] < current_min_dist {
                min_node = node.id.clone();
            }
            min_node
        })
    }

    pub fn dijkstra_shortest(graph: &Graph, start: &str, end: &str) -> Vec<Node> {
        let mut result: Vec<Node> = Vec::new();
        // if one of start and end is not in the graph, return empty vector
        if !graph.nodes_map.contains_key(start) || !graph.nodes_map.contains_key(end) {
            return result;
        }
        // queue is the nodes to exam
        // dist is a map of node id => distance from source to id
        let (mut queue, mut dist): (HashSet<Node>, HashMap<String, f64>) = graph
            .nodes
            .par_iter()
            .map(|n| {
                if n.id == start {
                    (n.clone(), (n.id.clone(), 0.0))
                } else {
                    (n.clone(), (n.id.clone(), INFINITY))
                }
            })
            .unzip();
        // node id => previous node id
        let mut prev: HashMap<String, String> = HashMap::new();

        while queue.len() > 0 {
            // minimum distance node
            let min_node = min_dist_node(&queue, &dist);
            if min_node == end {
                break;
            }
            if let Some(n) = graph.get_node(&min_node) {
                // remove from queue
                queue.remove(&n);
            } else {
                // no path found
                break;
            }
            graph.direct_connected(&min_node).iter().for_each(|n| {
                if queue.contains(&n) {
                    let mut alt = dist[&min_node];
                    if graph.weighted {
                        alt += graph.get_link(&min_node, &n.id).unwrap().weight;
                    } else {
                        alt += 1.0
                    }
                    if alt < dist[&n.id] {
                        dist.insert(n.id.clone(), alt);
                        prev.insert(n.id.clone(), min_node.to_string());
                    }
                }
            });
        }

        let mut c = end;
        // build path from node id => prev node id map
        // it's like a linked list
        loop {
            // if end is not even in the prev map, meaning there is no path
            if !prev.contains_key(c) && c != start {
                break;
            }
            // This unwrap is safe because all t is from prev,
            // which must be in the graph
            result.insert(0, graph.get_node(&c).unwrap().clone());
            if let Some(p) = prev.get(c) {
                // trace back to start
                if c == start {
                    break;
                }
                c = p;
            } else {
                // no parent node, meaning it should be the start node
                break;
            }
        }
        result
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_dijkstra_shortest() {
            let mut g = Graph::new();
            let n1 = Node::new("a".to_string(), "a".to_string());
            let n2 = Node::new("b".to_string(), "b".to_string());
            let n3 = Node::new("c".to_string(), "c".to_string());
            let n4 = Node::new("d".to_string(), "d".to_string());
            g.add_node(&n1);
            g.add_node(&n2);
            g.add_node(&n3);
            g.add_node(&n4);
            g.add_link(&Link {
                source: "a".to_string(),
                target: "b".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            g.add_link(&Link {
                source: "b".to_string(),
                target: "c".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            g.add_link(&Link {
                source: "c".to_string(),
                target: "d".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            g.add_link(&Link {
                source: "a".to_string(),
                target: "c".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            assert_eq!(
                dijkstra_shortest(&g, "a", "d"),
                vec![n1.clone(), n3, n4.clone()]
            );
            g.add_link(&Link {
                source: "a".to_string(),
                target: "d".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            assert_eq!(dijkstra_shortest(&g, "a", "d"), vec![n1, n4]);
            // TODO: test weighted
        }
    }
}
