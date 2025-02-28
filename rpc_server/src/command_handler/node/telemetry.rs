use crate::command_handler::RpcCommandHandler;
use anyhow::{anyhow, bail};
use rsnano_rpc_messages::{TelemetryArgs, TelemetryDto, TelemetryResponse};
use std::net::SocketAddrV6;

impl RpcCommandHandler {
    pub(crate) fn telemetry(&self, args: TelemetryArgs) -> anyhow::Result<TelemetryResponse> {
        let mut responses = Vec::new();
        if args.address.is_some() || args.port.is_some() {
            let endpoint = Self::get_endpoint(&args)?;

            if self.is_local_address(&endpoint) {
                // Requesting telemetry metrics locally
                let data = self.node.telemetry.local_telemetry();
                responses.push(TelemetryDto::from(data));
            } else {
                let telemetry = self
                    .node
                    .telemetry
                    .get_telemetry(&endpoint)
                    .ok_or_else(|| anyhow!("Peer not found"))?;

                responses.push(TelemetryDto::from(telemetry));
            }
        } else {
            // By default, local telemetry metrics are returned,
            // setting "raw" to true returns metrics from all nodes requested.
            let output_raw = args.raw.unwrap_or_default().inner();
            if output_raw {
                let all_telemetries = self.node.telemetry.get_all_telemetries();
                for (addr, data) in all_telemetries {
                    let mut metric = TelemetryDto::from(data);
                    metric.address = Some(addr.ip().clone());
                    metric.port = Some(addr.port().into());
                    responses.push(metric);
                }
            } else {
                // Default case without any parameters, requesting telemetry metrics locally
                let data = self.node.telemetry.local_telemetry();
                responses.push(data.into());
            }
        }

        Ok(TelemetryResponse { metrics: responses })
    }

    fn get_endpoint(args: &TelemetryArgs) -> anyhow::Result<SocketAddrV6> {
        let Some(address) = args.address else {
            bail!("Both port and address required");
        };
        let Some(port) = args.port else {
            bail!("Both port and address required");
        };

        Ok(SocketAddrV6::new(address, port.into(), 0, 0))
    }

    fn is_local_address(&self, addr: &SocketAddrV6) -> bool {
        addr.ip().is_loopback() && addr.port() == self.node.tcp_listener.local_address().port()
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use rsnano_rpc_messages::{RpcCommand, RpcError, TelemetryArgs};

    use crate::command_handler::test_rpc_command;

    #[test]
    fn fails_when_only_port_provided() {
        let cmd = RpcCommand::Telemetry(TelemetryArgs {
            raw: None,
            address: None,
            port: Some(123.into()),
        });
        let error: RpcError = test_rpc_command(cmd);
        assert_eq!(error.error, "Both port and address required")
    }

    #[test]
    fn fails_when_only_address_provided() {
        let cmd = RpcCommand::Telemetry(TelemetryArgs {
            raw: None,
            address: Some(Ipv6Addr::LOCALHOST),
            port: None,
        });
        let error: RpcError = test_rpc_command(cmd);
        assert_eq!(error.error, "Both port and address required")
    }
}
