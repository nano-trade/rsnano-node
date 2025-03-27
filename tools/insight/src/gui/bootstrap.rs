use eframe::egui::{self, CentralPanel, Label, ScrollArea};
use egui_extras::{Size, StripBuilder};

pub(crate) fn view_bootstrap(ctx: &egui::Context, model: BootstrapViewModel) {
    CentralPanel::default().show(ctx, |ui| {
        ScrollArea::vertical().show(ui, |ui| {
            StripBuilder::new(ui)
                .sizes(Size::remainder(), 2)
                .horizontal(|mut strip| {
                    strip.cell(|ui| {
                        ui.horizontal(|ui| {
                            ui.heading("Priority accounts: ");
                            ui.heading(model.priority_accounts);
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
                            ui.heading("Blocked accounts: ");
                            ui.heading(model.blocked_accounts);
                        });

                        for item in model.blocked {
                            ui.horizontal(|ui| {
                                StripBuilder::new(ui)
                                    .size(Size::exact(200.0))
                                    .size(Size::exact(200.0))
                                    .size(Size::exact(200.0))
                                    .horizontal(|mut strip| {
                                        strip.cell(|ui| {
                                            ui.label(item.account);
                                        });
                                        strip.cell(|ui| {
                                            ui.label(item.dependency);
                                        });
                                        strip.cell(|ui| {
                                            ui.label(item.dependency_account);
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
    pub priorities: Vec<PriorityViewModel>,
    pub blocked: Vec<BlockedViewModel>,
}

pub(crate) struct PriorityViewModel {
    pub account: String,
    pub priority: String,
}

pub(crate) struct BlockedViewModel {
    pub account: String,
    pub dependency: String,
    pub dependency_account: String,
}
