use eframe::egui::{self, CentralPanel};

pub(crate) fn view_block_processor(ctx: &egui::Context) {
    CentralPanel::default().show(ctx, |ui| {
        ui.label("TODO");
    });
}
