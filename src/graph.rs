pub mod graph {
    use std::collections::HashMap;
    extern crate rayon;
    use rayon::prelude::*;
    use std::collections::HashSet;
    use std::hash::Hash;
    use std::hash::Hasher;

    #[derive(Debug, Eq, Clone)]
    pub struct Node {
        pub id: String,
        pub name: String,
    }

    impl Node {
        pub fn new(id: String, name: String) -> Self {
            Node { id: id, name: name }
        }
    }

    impl PartialEq for Node {
        fn eq(&self, other: &Node) -> bool {
            self.id == other.id && self.name == other.name
        }
    }
    impl Hash for Node {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.id.hash(state);
        }
    }

    #[derive(Debug, Clone)]
    pub struct Link {
        pub source: String,
        pub target: String,
        pub label: String,
        pub weight: f64,
    }

    fn make_link_key(source: &str, target: &str) -> String {
        format!("{}_{}", &source, &target).to_string()
    }

    #[derive(Debug)]
    pub struct Graph {
        pub nodes: Vec<Node>,
        // node id to nodes index
        pub nodes_map: HashMap<String, usize>,
        pub links: HashMap<String, Link>,
        // if it's a directed graph, default is true
        pub directed: bool,
        pub weighted: bool,
    }

    // Graph construct related methods
    impl Graph {
        pub fn new() -> Self {
            Graph {
                nodes_map: HashMap::new(),
                links: HashMap::new(),
                nodes: Vec::new(),
                directed: true,
                weighted: false,
            }
        }
        // TODO: replace node
        pub fn add_node(&mut self, n: &Node) -> Result<bool, String> {
            // 如果节点已经存在，则不插入
            if self.nodes_map.contains_key(&n.id) {
                return Err(format!("[WARN] node{} is already existed, skipping", &n.id));
            }
            self.nodes_map.insert(n.id.clone(), self.nodes.len());
            self.nodes.push(n.clone());
            Ok(true)
        }
        // TODO: replace link
        pub fn add_link(&mut self, l: &Link) -> Result<bool, String> {
            if !self.nodes_map.contains_key(&l.source) {
                self.add_node(&Node {
                    id: l.source.clone(),
                    name: "".to_string(),
                })?;
            }
            if !self.nodes_map.contains_key(&l.target) {
                self.add_node(&Node {
                    id: l.target.clone(),
                    name: "".to_string(),
                })?;
            }
            let key = make_link_key(&l.source, &l.target);
            if self.links.contains_key(&key) {
                Err(format!(
                    "[WARN] link {} to {} is already existed, skipping",
                    l.source, l.target
                ))
            } else {
                self.links.insert(key, l.clone());
                Ok(true)
            }
        }
    }

    // Graph queries works both on directed graph and undirected graph
    impl Graph {
        pub fn to_matrix(&self) -> Vec<Vec<bool>> {
            self.links.iter().fold(
                vec![vec![false; self.nodes.len()]; self.nodes.len()],
                |mut rows, (_, l)| {
                    rows[self.nodes_map[&l.source]][self.nodes_map[&l.target]] = true;
                    if !self.directed {
                        rows[self.nodes_map[&l.target]][self.nodes_map[&l.source]] = true;
                    }
                    rows
                },
            )
        }

        pub fn direct_connected(&self, source_id: &str) -> Vec<Node> {
            if !self.nodes_map.contains_key(source_id) {
                return Vec::new();
            }
            let m = self.to_matrix();
            let source_idx = self.nodes_map[source_id];
            m[source_idx]
                .par_iter()
                .enumerate()
                .filter_map(|(idx, &is_connected)| {
                    if is_connected {
                        Some(self.nodes[idx].clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<Node>>()
        }

        pub fn get_node(&self, id: &str) -> Option<Node> {
            if self.nodes_map.contains_key(id) {
                Some(self.nodes[self.nodes_map[id]].clone())
            } else {
                None
            }
        }

        pub fn get_link(&self, source: &str, target: &str) -> Option<Link> {
            let key = &make_link_key(source, target);
            if self.links.contains_key(key) {
                Some(self.links[key].clone())
            } else {
                None
            }
        }

        // finds all connected components in a graph
        // graph must be an undirected graph, otherwise it will panic
        pub fn connected_components(&mut self) -> Vec<Vec<Node>> {
            // set directed state, set back when done
            let cur_directed = self.directed;
            self.directed = false;
            if self.nodes.len() == 0 {
                self.directed = cur_directed;
                return Vec::new();
            }
            let mut openset: HashSet<Node> = self.nodes.iter().map(|n| n.clone()).collect();
            let mut components: HashMap<String, Vec<Node>> = HashMap::new();
            let mut start_node = self.nodes[0].clone();
            loop {
                openset.remove(&start_node);
                components.insert(start_node.id.clone(), vec![start_node.clone()]);
                loop {
                    let mut direct_connected_nodes: Vec<Node> = self
                        .direct_connected(&start_node.id)
                        .iter()
                        .filter(|n| openset.contains(&n))
                        .cloned()
                        .collect();
                    if direct_connected_nodes.len() > 0 {
                        direct_connected_nodes.iter().for_each(|n| {
                            openset.remove(&n);
                        });
                        components
                            .get_mut(&start_node.id)
                            .unwrap()
                            .append(&mut direct_connected_nodes);
                    } else {
                        break;
                    }
                }
                if let Some(n) = openset.iter().next() {
                    start_node = n.clone();
                } else {
                    break;
                }
            }
            self.directed = cur_directed;
            components
                .iter()
                .map(|(_, component)| component)
                .cloned()
                .collect()
        }
    }

    // Graph queries only works on directed graph
    impl Graph {
        pub fn indegree(&mut self, node_id: &str) -> usize {
            if !self.nodes_map.contains_key(node_id) {
                return 0;
            }
            let node_idx = self.nodes_map[node_id];
            // set directed state, set back when done
            let cur_directed = self.directed;
            if !self.directed {
                self.directed = true;
            }
            let m = self.to_matrix();
            self.directed = cur_directed;
            m.iter().fold(0, |mut indegree, row| {
                if row[node_idx] {
                    indegree += 1;
                }
                indegree
            })
        }

        pub fn outdegree(&mut self, node_id: &str) -> usize {
            if !self.nodes_map.contains_key(node_id) {
                return 0;
            }
            let node_idx = self.nodes_map[node_id];
            // set directed state, set back when done
            let cur_directed = self.directed;
            if !self.directed {
                self.directed = true;
            }
            let m = self.to_matrix();
            self.directed = cur_directed;
            m[node_idx]
                .iter()
                .filter(|&&is_connected| is_connected)
                .cloned()
                .collect::<Vec<bool>>()
                .len()
        }

        pub fn degree_centrality(&mut self, node_id: &str) -> usize {
            self.indegree(node_id) + self.outdegree(node_id)
        }

        pub fn pagerank_centrality(&self, node_id: &str) -> f64 {
            0.0
        }
    }

    #[cfg(test)]
    mod tests {
        // Note this useful idiom: importing names from outer (for mod tests) scope.
        use super::*;

        fn help_create_test_directed_graph() -> Graph {
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
                source: "c".to_string(),
                target: "d".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            g
        }

        #[test]
        fn test_create_graph() {
            let g = Graph::new();
            assert_eq!(g.weighted, false);
            assert_eq!(g.directed, true);
            assert_eq!(g.nodes.len(), 0);
            assert_eq!(g.nodes_map.len(), 0);
            assert_eq!(g.links.len(), 0);
        }

        #[test]
        fn test_new_node() {
            let n1 = Node::new("1".to_string(), "1".to_string());
            let n2 = Node {
                id: "1".to_string(),
                name: "1".to_string(),
            };
            assert_eq!(n1, n2);
        }

        #[test]
        fn test_clone_node() {
            let n1 = Node::new("1".to_string(), "1".to_string());
            let n2 = n1.clone();
            assert_eq!(n1, n2);
        }

        #[test]
        fn test_clone_link() {
            let l1 = Link {
                source: "1".to_string(),
                target: "2".to_string(),
                label: "1".to_string(),
                weight: 1.0,
            };
            let l2 = l1.clone();
            assert_eq!(l1.source, l2.source);
            assert_eq!(l1.target, l2.target);
            assert_eq!(l1.label, l2.label);
            assert_eq!(l1.weight, l2.weight);
        }

        #[test]
        fn test_add_node() {
            let mut g = Graph::new();
            let n1 = Node::new("1".to_string(), "1".to_string());
            g.add_node(&n1);
            assert_eq!(g.get_node(&n1.id).unwrap(), n1);
            // same nodes will not duplicate
            g.add_node(&n1);
            assert_eq!(g.nodes.len(), 1);
            assert_eq!(g.nodes_map.len(), 1);
        }

        #[test]
        fn test_add_link() {
            let mut g = Graph::new();
            let l1 = Link {
                source: "1".to_string(),
                target: "2".to_string(),
                label: "1".to_string(),
                weight: 1.0,
            };
            g.add_link(&l1);
            assert_eq!(g.nodes.len(), 2);
            assert_eq!(g.nodes_map.len(), 2);
            assert_eq!(g.links.len(), 1);
            assert_eq!(g.get_node("1").unwrap().id, "1");
            assert_eq!(g.get_node("2").unwrap().id, "2");
            assert_eq!(g.get_link("1", "2").unwrap().source, "1");
            assert_eq!(g.get_link("1", "2").unwrap().target, "2");
        }

        #[test]
        fn test_to_matrix() {
            let mut g = Graph::new();
            let l1 = Link {
                source: "1".to_string(),
                target: "2".to_string(),
                label: "1".to_string(),
                weight: 1.0,
            };
            g.add_link(&l1);
            assert_eq!(g.to_matrix(), vec![vec![false, true], vec![false, false]]);
            g.directed = false;
            assert_eq!(g.to_matrix(), vec![vec![false, true], vec![true, false]]);
        }

        #[test]
        fn test_direct_connected() {
            let mut g = Graph::new();
            g.add_link(&Link {
                source: "1".to_string(),
                target: "2".to_string(),
                label: "1".to_string(),
                weight: 1.0,
            });
            g.add_link(&Link {
                source: "1".to_string(),
                target: "3".to_string(),
                label: "1".to_string(),
                weight: 1.0,
            });
            assert_eq!(
                g.direct_connected("1"),
                vec![
                    Node {
                        id: "2".to_string(),
                        name: "".to_string()
                    },
                    Node {
                        id: "3".to_string(),
                        name: "".to_string()
                    }
                ]
            );
            assert_eq!(g.direct_connected("2"), vec![]);
            g.directed = false;
            assert_eq!(
                g.direct_connected("2"),
                vec![Node {
                    id: "1".to_string(),
                    name: "".to_string()
                },]
            );
        }

        #[test]
        fn test_connected_components_ok() {
            let mut g = Graph::new();
            g.directed = false;
            assert_eq!(g.connected_components().len(), 0);
            assert_eq!(g.directed, false);
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
                source: "c".to_string(),
                target: "d".to_string(),
                label: "".to_string(),
                weight: 1.0,
            });
            g.directed = true;
            assert_eq!(g.connected_components().len(), 2);
            assert_eq!(g.directed, true);
        }

        #[test]
        fn test_indegree() {
            let mut g = help_create_test_directed_graph();
            assert_eq!(g.indegree("a"), 0);
            assert_eq!(g.indegree("b"), 1);
        }

        #[test]
        fn test_outdegree() {
            let mut g = help_create_test_directed_graph();
            assert_eq!(g.outdegree("a"), 1);
            assert_eq!(g.outdegree("b"), 0);
        }

        #[test]
        fn test_degree_centrality() {
            let mut g = help_create_test_directed_graph();
            assert_eq!(g.degree_centrality("a"), 1);
            assert_eq!(g.degree_centrality("b"), 1);
        }

        #[test]
        fn test_pagerank_centrality() {
            let mut g = help_create_test_directed_graph();
            assert_eq!(g.pagerank_centrality("a"), 0.0);
        }
    }
}
