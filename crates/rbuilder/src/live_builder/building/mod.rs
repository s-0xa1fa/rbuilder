use std::{marker::PhantomData, sync::Arc, time::Duration};

use crate::{
    building::{
        builders::{
            bob_builder::BobBuilder, BlockBuildingAlgorithm, BlockBuildingAlgorithmInput,
            UnfinishedBlockBuildingSink, UnfinishedBlockBuildingSinkFactory,
        },
        BlockBuildingContext,
    },
    live_builder::block_output::block_router::UnfinishedBlockRouter,
    live_builder::simulation::SlotOrderSimResults,
    roothash::run_trie_prefetcher,
};
use reth_db::Database;
use reth_provider::{BlockReader, DatabaseProviderFactory, StateProviderFactory};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use super::{
    order_input::{
        self, order_replacement_manager::OrderReplacementManager, orderpool::OrdersForBlock,
    },
    payload_events,
    simulation::OrderSimulationPool,
};

#[derive(Debug)]
pub struct BlockBuildingPool<P, DB> {
    provider: P,
    builders: Vec<Arc<dyn BlockBuildingAlgorithm<P, DB>>>,
    bob_builder: Option<BobBuilder>,
    sink_factory: Box<dyn UnfinishedBlockBuildingSinkFactory>,
    orderpool_subscriber: order_input::OrderPoolSubscriber,
    order_simulation_pool: OrderSimulationPool<P>,
    run_sparse_trie_prefetcher: bool,
    phantom: PhantomData<DB>,
}

impl<P, DB> BlockBuildingPool<P, DB>
where
    DB: Database + Clone + 'static,
    P: DatabaseProviderFactory<DB = DB, Provider: BlockReader>
        + StateProviderFactory
        + Clone
        + 'static,
{
    pub async fn new(
        provider: P,
        builders: Vec<Arc<dyn BlockBuildingAlgorithm<P, DB>>>,
        bob_builder: Option<BobBuilder>,
        sink_factory: Box<dyn UnfinishedBlockBuildingSinkFactory>,
        orderpool_subscriber: order_input::OrderPoolSubscriber,
        order_simulation_pool: OrderSimulationPool<P>,
        run_sparse_trie_prefetcher: bool,
    ) -> Self {
        BlockBuildingPool {
            provider,
            builders,
            bob_builder,
            sink_factory,
            orderpool_subscriber,
            order_simulation_pool,
            run_sparse_trie_prefetcher,
            phantom: PhantomData,
        }
    }

    /// Connects OrdersForBlock->OrderReplacementManager->Simulations and calls start_building_job
    pub fn start_block_building(
        &mut self,
        payload: payload_events::MevBoostSlotData,
        block_ctx: BlockBuildingContext,
        global_cancellation: CancellationToken,
        max_time_to_build: Duration,
    ) {
        let block_cancellation = global_cancellation.child_token();

        let cancel = block_cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(max_time_to_build).await;
            cancel.cancel();
        });

        let (orders_for_block, sink) = OrdersForBlock::new_with_sink();
        // add OrderReplacementManager to manage replacements and cancellations
        let order_replacement_manager = OrderReplacementManager::new(Box::new(sink));
        // sink removal is automatic via OrderSink::is_alive false
        let _block_sub = self.orderpool_subscriber.add_sink(
            block_ctx.block_env.number.to(),
            Box::new(order_replacement_manager),
        );

        let slot_timestamp = payload.timestamp();
        let simulations_for_block = self.order_simulation_pool.spawn_simulation_job(
            block_ctx.clone(),
            orders_for_block,
            block_cancellation.clone(),
        );

        let sink = match self.bob_builder.clone() {
            Some(builder) => Arc::new(UnfinishedBlockRouter::new(
                self.sink_factory
                    .create_sink(payload.clone(), block_cancellation.clone()),
                builder.server(),
                slot_timestamp,
                builder.new_handle(block_cancellation.clone()),
                builder.config.stream_start_dur,
            )),
            None => self
                .sink_factory
                .create_sink(payload.clone(), block_cancellation.clone()),
        };
        self.start_building_job(
            block_ctx,
            sink,
            simulations_for_block,
            block_cancellation.clone(),
        );
    }

    /// Per each BlockBuildingAlgorithm creates BlockBuildingAlgorithmInput and Sinks and spawn a task to run it
    fn start_building_job(
        &mut self,
        ctx: BlockBuildingContext,
        builder_sink: Arc<dyn UnfinishedBlockBuildingSink>,
        input: SlotOrderSimResults,
        cancel: CancellationToken,
    ) {
        let (broadcast_input, _) = broadcast::channel(10_000);

        let block_number = ctx.block_env.number.to::<u64>();

        for builder in self.builders.iter() {
            let builder_name = builder.name();
            debug!(block = block_number, builder_name, "Spawning builder job");
            let input = BlockBuildingAlgorithmInput::<P> {
                provider: self.provider.clone(),
                ctx: ctx.clone(),
                input: broadcast_input.subscribe(),
                sink: builder_sink.clone(),
                cancel: cancel.clone(),
            };
            let builder = builder.clone();
            tokio::task::spawn_blocking(move || {
                builder.build_blocks(input);
                debug!(block = block_number, builder_name, "Stopped builder job");
            });
        }

        if self.run_sparse_trie_prefetcher {
            let input = broadcast_input.subscribe();
            let provider = self.provider.clone();
            tokio::task::spawn_blocking(move || {
                run_trie_prefetcher(
                    ctx.attributes.parent,
                    ctx.shared_sparse_mpt_cache,
                    provider,
                    input,
                    cancel.clone(),
                );
                debug!(block = block_number, "Stopped trie prefetcher job");
            });
        }

        tokio::spawn(multiplex_job(input.orders, broadcast_input));
    }
}

async fn multiplex_job<T>(mut input: mpsc::Receiver<T>, sender: broadcast::Sender<T>) {
    // we don't worry about waiting for input forever because it will be closed by producer job
    while let Some(input) = input.recv().await {
        // we don't create new subscribers to the broadcast so here we can be sure that err means end of receivers
        if sender.send(input).is_err() {
            return;
        }
    }
    trace!("Cancelling multiplex job");
}
