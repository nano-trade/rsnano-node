use crate::command_handler::RpcCommandHandler;
use rsnano_core::utils::Peer;
use rsnano_rpc_messages::{AddressWithPortArgs, SuccessResponse};

impl RpcCommandHandler {
    pub(crate) fn work_peer_add(&self, args: AddressWithPortArgs) -> SuccessResponse {
        self.node
            .work_factory
            .add_peer(Peer::new(args.address, args.port.into()));
        SuccessResponse::new()
    }
}
