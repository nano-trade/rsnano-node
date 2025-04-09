# Vote rebroadcast

After the node has processed a vote, that vote may be rebroadcasted to more nodes in the network.

![rebroadcast diagram](http://www.plantuml.com/plantuml/proxy?cache=no&fmt=svg&src=https://raw.github.com/rsnano-node/rsnano-node/develop/node/src/vote_consensus/rebroadcast/doc/rebroadcast.puml)

The procedure is as follows:

1. The AEC event processer gets notified that a vote was processed and calls `try_enqueue` on the `VoteRebroadcastQueue` 
2. The `VoteRebroadcaster` dequeues the vote and passes it into the `RebroadcastProcessor`
3. The `RebroadcastProcessor` decides whether to republish the vote. It uses the `WalletRepsCache` for this. This cache knowns whether the node hosts a representative.


