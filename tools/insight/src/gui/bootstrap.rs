use eframe::egui::{self, CentralPanel, ScrollArea, TextEdit};
use egui_extras::{Size, StripBuilder};
use rsnano_core::Account;

use crate::app::InsightApp;

pub(crate) fn view_bootstrap(ctx: &egui::Context, model: BootstrapViewModel, app: &mut InsightApp) {
    CentralPanel::default().show(ctx, |ui| {
        ScrollArea::vertical().auto_shrink(false).show(ui, |ui| {
            ui.add(
                TextEdit::singleline(&mut app.bootstrap.search)
                    .hint_text("filter account...")
                    .desired_width(500.0),
            );
            StripBuilder::new(ui)
                .sizes(Size::remainder(), 2)
                .horizontal(|mut strip| {
                    strip.cell(|ui| {
                        ui.horizontal(|ui| {
                            ui.heading("Priority accounts: ");
                            ui.heading(model.priority_accounts);
                        });

                        ui.horizontal(|ui| {
                            StripBuilder::new(ui)
                                .size(Size::exact(40.0))
                                .size(Size::remainder())
                                .horizontal(|mut strip| {
                                    strip.cell(|ui| {
                                        ui.strong("Priority");
                                    });
                                    strip.cell(|ui| {
                                        ui.strong("Account");
                                    });
                                });
                        });

                        for item in model.priorities {
                            ui.horizontal(|ui| {
                                StripBuilder::new(ui)
                                    .size(Size::exact(40.0))
                                    .size(Size::remainder())
                                    .horizontal(|mut strip| {
                                        strip.cell(|ui| {
                                            ui.label(item.priority);
                                        });
                                        strip.cell(|ui| {
                                            ui.label(item.account);
                                        });
                                    });
                            });
                        }
                    });

                    strip.cell(|ui| {
                        ui.horizontal(|ui| {
                            ui.heading(format!("Blocked accounts: {}", model.blocked_accounts));
                        });
                        ui.horizontal(|ui| {
                            ui.label(format!("known senders: {}", model.known_dependencies));
                            ui.add_space(50.0);
                            ui.label(format!(
                                "unique senders: {}",
                                model.unique_blocking_accounts
                            ));
                            ui.add_space(50.0);
                            ui.label(format!("reinsertable: {}", model.reinsertable));
                        });

                        ui.horizontal(|ui| {
                            StripBuilder::new(ui)
                                .size(Size::exact(200.0))
                                .size(Size::exact(200.0))
                                .size(Size::exact(200.0))
                                .horizontal(|mut strip| {
                                    strip.cell(|ui| {
                                        ui.strong("Blocked account");
                                    });
                                    strip.cell(|ui| {
                                        ui.strong("Missing send");
                                    });
                                    strip.cell(|ui| {
                                        ui.strong("Sender");
                                    });
                                });
                        });
                        for item in model.blocked {
                            ui.horizontal(|ui| {
                                StripBuilder::new(ui)
                                    .size(Size::exact(200.0))
                                    .size(Size::exact(200.0))
                                    .size(Size::exact(200.0))
                                    .horizontal(|mut strip| {
                                        strip.cell(|ui| {
                                            if ui.link(item.account).clicked() {
                                                app.bootstrap.search =
                                                    item.account_val.encode_account();
                                            }
                                        });
                                        strip.cell(|ui| {
                                            ui.label(item.dependency);
                                        });
                                        strip.cell(|ui| {
                                            if ui.link(item.dependency_account).clicked() {
                                                app.bootstrap.search =
                                                    item.dependency_account_val.encode_account();
                                            }
                                        });
                                    });
                            });
                        }
                    });
                });
        });
    });
}

pub(crate) struct BootstrapViewModel {
    pub priority_accounts: String,
    pub blocked_accounts: String,
    pub unique_blocking_accounts: usize,
    pub known_dependencies: usize,
    pub reinsertable: usize,
    pub priorities: Vec<PriorityViewModel>,
    pub blocked: Vec<BlockedViewModel>,
    pub search: String,
}

pub(crate) struct PriorityViewModel {
    pub account: String,
    pub priority: String,
}

pub(crate) struct BlockedViewModel {
    pub account: String,
    pub dependency: String,
    pub dependency_account: String,
    pub account_val: Account,
    pub dependency_account_val: Account,
}
