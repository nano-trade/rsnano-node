use eframe::egui::{
    self, global_theme_preference_switch, warn_if_debug_build, CentralPanel, TopBottomPanel,
};

use super::{
    block_processor::view_block_processor,
    bootstrap::{view_bootstrap, BlockedViewModel, BootstrapViewModel, PriorityViewModel},
    formatted_number, view_frontier_scan, view_ledger_stats, view_message_recorder_controls,
    view_message_tab, view_node_runner, view_peers, view_queue_group, view_search_bar, view_tabs,
    BlockViewModel, ChannelsViewModel, ExplorerView, FrontierScanViewModel, MessageStatsView,
    MessageStatsViewModel, MessageTableViewModel, QueueGroupViewModel, TabViewModel,
};
use crate::{app::InsightApp, explorer::ExplorerState, gui::QueueViewModel, navigator::NavItem};

pub(crate) struct MainView {
    model: MainViewModel,
}

impl MainView {
    pub(crate) fn new() -> Self {
        let model = MainViewModel::new();
        Self { model }
    }
}

impl MainView {
    fn view_controls_panel(&mut self, ctx: &egui::Context) {
        TopBottomPanel::top("controls_panel").show(ctx, |ui| {
            ui.add_space(1.0);
            ui.horizontal(|ui| {
                view_node_runner(ui, &mut self.model.app.node_runner);
                ui.separator();
                view_message_recorder_controls(ui, &self.model.app.msg_recorder);
                ui.separator();
                view_search_bar(ui, &mut self.model.search_input, &mut self.model.app);
            });
            ui.add_space(1.0);
        });
    }

    fn view_tabs(&mut self, ctx: &egui::Context) {
        TopBottomPanel::top("tabs_panel").show(ctx, |ui| {
            view_tabs(ui, &self.model.tabs(), &mut self.model.app.navigator);
        });
    }

    fn view_stats(&mut self, ctx: &egui::Context) {
        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                global_theme_preference_switch(ui);
                ui.separator();
                MessageStatsView::new(self.model.message_stats()).view(ui);
                ui.separator();
                view_ledger_stats(ui, &self.model.app.ledger_stats);
                warn_if_debug_build(ui);
            });
        });
    }
}

impl eframe::App for MainView {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.model.update();
        self.view_controls_panel(ctx);
        self.view_tabs(ctx);
        self.view_stats(ctx);

        match self.model.app.navigator.current {
            NavItem::Peers => view_peers(ctx, self.model.channels()),
            NavItem::Messages => view_message_tab(ctx, &mut self.model),
            NavItem::Queues => view_queues(ctx, self.model.queue_groups()),
            NavItem::BlockProcessor => view_block_processor(ctx),
            NavItem::Bootstrap => view_bootstrap(ctx, self.model.bootstrap(), &mut self.model.app),
            NavItem::FrontierScan => {
                view_frontier_scan(ctx, self.model.frontier_scan(), &mut self.model.app)
            }
            NavItem::Explorer => {
                ExplorerView::new(&self.model.explorer(), &mut self.model.app).show(ctx)
            }
        }

        // Repaint to show the continuously increasing current block and message counters
        ctx.request_repaint();
    }
}

fn view_queues(ctx: &egui::Context, groups: Vec<QueueGroupViewModel>) {
    CentralPanel::default().show(ctx, |ui| {
        for group in groups {
            view_queue_group(ui, group);
            ui.add_space(10.0);
        }
    });
}

pub(crate) struct MainViewModel {
    pub app: InsightApp,
    pub message_table: MessageTableViewModel,
    pub search_input: String,
}

impl MainViewModel {
    pub(crate) fn new() -> Self {
        let app = InsightApp::new();
        Self::for_app(app)
    }

    pub(crate) fn for_app(app: InsightApp) -> Self {
        let message_table = MessageTableViewModel::new(app.messages.clone());

        Self {
            app,
            message_table,
            search_input: String::new(),
        }
    }

    pub(crate) fn update(&mut self) {
        if !self.app.update() {
            return;
        }

        self.message_table.update_message_counts();
    }

    pub(crate) fn tabs(&self) -> Vec<TabViewModel> {
        self.app
            .navigator
            .all
            .iter()
            .map(|i| TabViewModel {
                selected: *i == self.app.navigator.current,
                label: i.name(),
                value: *i,
            })
            .collect()
    }

    pub(crate) fn message_stats(&self) -> MessageStatsViewModel {
        MessageStatsViewModel::new(&self.app.msg_recorder)
    }

    pub(crate) fn channels(&mut self) -> ChannelsViewModel {
        ChannelsViewModel::new(&mut self.app.channels)
    }

    pub(crate) fn queue_groups(&self) -> Vec<QueueGroupViewModel> {
        vec![
            QueueGroupViewModel {
                heading: "Active Elections".to_string(),
                queues: vec![
                    QueueViewModel::new(
                        "Priority",
                        self.app.aec_info.priority,
                        self.app.aec_info.max_elections,
                    ),
                    QueueViewModel::new("Hinted", self.app.aec_info.hinted, self.app.max_hinted),
                    QueueViewModel::new(
                        "Optimistic",
                        self.app.aec_info.optimistic,
                        self.app.max_optimistic,
                    ),
                    QueueViewModel::new(
                        "Total",
                        self.app.aec_info.total,
                        self.app.aec_info.max_elections,
                    ),
                ],
            },
            QueueGroupViewModel::for_fair_queue("Block Processor", &self.app.block_processor_info),
            QueueGroupViewModel::for_fair_queue("Vote Processor", &self.app.vote_processor_info),
            QueueGroupViewModel {
                heading: "Miscellaneous".to_string(),
                queues: vec![QueueViewModel::new(
                    "Confirming",
                    self.app.confirming_set.size,
                    self.app.confirming_set.max_size,
                )],
            },
        ]
    }

    pub fn bootstrap(&self) -> BootstrapViewModel {
        let priorities = self
            .app
            .bootstrap
            .priorities
            .iter()
            .map(|(prio, account)| PriorityViewModel {
                account: account.encode_account(),
                priority: format!("{:.2}", prio.as_f64()),
            })
            .collect();

        let blocked = self
            .app
            .bootstrap
            .blocked
            .iter()
            .map(|i| {
                let mut model = BlockedViewModel {
                    account: i.account.encode_account(),
                    dependency: i.dependency.to_string(),
                    dependency_account: i.dependency_account.encode_account(),
                    account_val: i.account,
                    dependency_account_val: i.dependency_account,
                };
                truncate_text(&mut model.account, 20);
                truncate_text(&mut model.dependency, 20);
                truncate_text(&mut model.dependency_account, 20);
                model
            })
            .collect();

        BootstrapViewModel {
            priority_accounts: formatted_number(self.app.bootstrap.priority_accounts),
            blocked_accounts: formatted_number(self.app.bootstrap.blocked_accounts),
            unique_blocking_accounts: self.app.bootstrap.unique_blocking_accounts,
            known_dependencies: self.app.bootstrap.known_dependencies,
            reinsertable: self.app.bootstrap.reinsertable,
            priorities,
            blocked,
            search: self.app.bootstrap.search.clone(),
        }
    }

    pub fn explorer(&self) -> BlockViewModel {
        let mut view_model = BlockViewModel::default();
        if let ExplorerState::Block(b) = self.app.explorer.state() {
            view_model.show(b);
        }
        view_model
    }

    pub fn frontier_scan(&self) -> FrontierScanViewModel {
        FrontierScanViewModel::new(&self.app.frontier_scan)
    }
}

fn truncate_text(s: &mut String, len: usize) {
    if s.len() > len {
        s.replace_range(len.., "...");
    }
}
