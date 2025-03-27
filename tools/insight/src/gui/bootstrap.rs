use eframe::egui::{self, CentralPanel};

pub(crate) fn view_bootstrap(ctx: &egui::Context, model: BootstrapViewModel) {
    CentralPanel::default().show(ctx, |ui| {
        ui.horizontal(|ui| {
            ui.heading("Priority accounts: ");
            ui.heading(model.priority_accounts);
            ui.add_space(25.0);
            ui.heading("Blocked accounts: ");
            ui.heading(model.blocked_accounts);
        })
    });
}

pub(crate) struct BootstrapViewModel {
    pub priority_accounts: String,
    pub blocked_accounts: String,
}
