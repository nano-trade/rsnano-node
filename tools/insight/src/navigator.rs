#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum NavItem {
    Peers,
    Messages,
    Queues,
    Bootstrap,
    FrontierScan,
    Explorer,
}

impl NavItem {
    pub fn name(&self) -> &'static str {
        match self {
            NavItem::Peers => "Peers",
            NavItem::Messages => "Messages",
            NavItem::Queues => "Queues",
            NavItem::Bootstrap => "Bootstrap",
            NavItem::FrontierScan => "Frontier Scan",
            NavItem::Explorer => "Explorer",
        }
    }
}

static NAV_ORDER: [NavItem; 6] = [
    NavItem::Peers,
    NavItem::Messages,
    NavItem::Queues,
    NavItem::Bootstrap,
    NavItem::FrontierScan,
    NavItem::Explorer,
];

pub(crate) struct Navigator {
    pub current: NavItem,
    pub all: Vec<NavItem>,
}

impl Navigator {
    pub(crate) fn new() -> Self {
        Self {
            current: NavItem::Peers,
            all: NAV_ORDER.into(),
        }
    }
}
