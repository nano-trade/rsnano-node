use serde_json::json;
use std::{
    ops::Deref,
    sync::{Arc, Mutex, RwLock},
};

#[derive(PartialEq, Eq, Debug)]
pub struct ContainerSize {
    pub count: usize,
    pub element_size: usize,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Leaf {
    pub name: String,
    pub info: ContainerSize,
}

impl Leaf {
    fn into_json(self) -> (String, serde_json::Value) {
        let fields = json!(
        {
            "count": self.info.count.to_string(),
            "size": self.info.element_size.to_string()
        });
        (self.name, fields)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct Node {
    pub name: String,
    pub children: ContainerInfo,
}

impl Node {
    fn into_json(self) -> (String, serde_json::Value) {
        let mut children = serde_json::Map::new();
        for child in self.children.0 {
            let (name, value) = child.into_json();
            children.insert(name, value);
        }
        (self.name, serde_json::Value::Object(children))
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum ContainerInfoEntry {
    Leaf(Leaf),
    Node(Node),
}

impl ContainerInfoEntry {
    fn into_json(self) -> (String, serde_json::Value) {
        match self {
            ContainerInfoEntry::Leaf(leaf) => leaf.into_json(),
            ContainerInfoEntry::Node(node) => node.into_json(),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ContainerInfo(Vec<ContainerInfoEntry>);

impl ContainerInfo {
    pub fn builder() -> ContainerInfosBuilder {
        ContainerInfosBuilder(Vec::new())
    }

    pub fn into_json(self) -> serde_json::Value {
        let mut data = serde_json::Map::new();
        for entry in self.0 {
            let (name, value) = entry.into_json();
            data.insert(name, value);
        }
        serde_json::Value::Object(data)
    }
}

impl Deref for ContainerInfo {
    type Target = Vec<ContainerInfoEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct ContainerInfosBuilder(Vec<ContainerInfoEntry>);

impl ContainerInfosBuilder {
    pub fn leaf(mut self, name: impl Into<String>, count: usize, element_size: usize) -> Self {
        self.0.push(ContainerInfoEntry::Leaf(Leaf {
            name: name.into(),
            info: ContainerSize {
                count,
                element_size,
            },
        }));
        self
    }

    pub fn node(mut self, name: impl Into<String>, infos: ContainerInfo) -> Self {
        self.0.push(ContainerInfoEntry::Node(Node {
            name: name.into(),
            children: infos,
        }));
        self
    }
    pub fn finish(self) -> ContainerInfo {
        ContainerInfo(self.0)
    }
}

impl<const N: usize> From<[(&'static str, usize, usize); N]> for ContainerInfo {
    fn from(value: [(&'static str, usize, usize); N]) -> Self {
        let mut builder = ContainerInfo::builder();
        for (name, count, element_size) in value {
            builder = builder.leaf(name, count, element_size);
        }
        builder.finish()
    }
}

pub trait ContainerInfoProvider {
    fn container_info(&self) -> ContainerInfo;
}

#[derive(Default)]
pub struct ContainerInfoFactory(Vec<FactoryEntry>);

struct FactoryEntry {
    name: String,
    factory: FactoryType,
}

enum FactoryType {
    Leaf(Box<dyn Fn() -> usize + Send + Sync>),
    Node(Box<dyn ContainerInfoProvider + Send + Sync>),
}

impl ContainerInfoFactory {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_leaf(
        &mut self,
        name: impl Into<String>,
        get_size: impl Fn() -> usize + Send + Sync + 'static,
    ) {
        self.0.push(FactoryEntry {
            name: name.into(),
            factory: FactoryType::Leaf(Box::new(get_size)),
        });
    }

    pub fn add(
        &mut self,
        name: impl Into<String>,
        node: impl ContainerInfoProvider + Send + Sync + 'static,
    ) {
        self.0.push(FactoryEntry {
            name: name.into(),
            factory: FactoryType::Node(Box::new(node)),
        });
    }
}

impl ContainerInfoProvider for ContainerInfoFactory {
    fn container_info(&self) -> ContainerInfo {
        let mut builder = ContainerInfo::builder();
        for entry in &self.0 {
            match &entry.factory {
                FactoryType::Leaf(f) => {
                    builder = builder.leaf(entry.name.clone(), f(), 0);
                }
                FactoryType::Node(f) => {
                    builder = builder.node(entry.name.clone(), f.container_info());
                }
            }
        }
        builder.finish()
    }
}

impl<T> ContainerInfoProvider for Arc<T>
where
    T: ContainerInfoProvider,
{
    fn container_info(&self) -> ContainerInfo {
        self.as_ref().container_info()
    }
}

impl<T> ContainerInfoProvider for Arc<Mutex<T>>
where
    T: ContainerInfoProvider,
{
    fn container_info(&self) -> ContainerInfo {
        self.lock().unwrap().container_info()
    }
}

impl<T> ContainerInfoProvider for Arc<RwLock<T>>
where
    T: ContainerInfoProvider,
{
    fn container_info(&self) -> ContainerInfo {
        self.read().unwrap().container_info()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_container_info_factory() {
        let factory = ContainerInfoFactory::new();
        let info = factory.container_info();
        assert!(info.is_empty());
    }

    #[test]
    fn add_one() {
        let mut factory = ContainerInfoFactory::new();
        factory.add_leaf("foo", || 123);
        let info = factory.container_info();
        assert_eq!(
            *info,
            [ContainerInfoEntry::Leaf(Leaf {
                name: "foo".to_string(),
                info: ContainerSize {
                    count: 123,
                    element_size: 0
                }
            })]
        );
    }

    #[test]
    fn add_multiple() {
        let mut factory = ContainerInfoFactory::new();
        factory.add_leaf("foo", || 123);
        factory.add_leaf("bar", || 456);
        factory.add_leaf("test", || 0);
        let info = factory.container_info();
        assert_eq!(
            *info,
            [
                ContainerInfoEntry::Leaf(Leaf {
                    name: "foo".to_string(),
                    info: ContainerSize {
                        count: 123,
                        element_size: 0
                    }
                }),
                ContainerInfoEntry::Leaf(Leaf {
                    name: "bar".to_string(),
                    info: ContainerSize {
                        count: 456,
                        element_size: 0
                    }
                }),
                ContainerInfoEntry::Leaf(Leaf {
                    name: "test".to_string(),
                    info: ContainerSize {
                        count: 0,
                        element_size: 0
                    }
                })
            ]
        );
    }
}
