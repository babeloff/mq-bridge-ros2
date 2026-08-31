use std::{
    sync::{Arc, Mutex, MutexGuard},
    thread::JoinHandle,
};

use rclrs::{
    Context, CreateBasicExecutor, ExecutorCommands, InitOptions, IntoNodeOptions, Node,
    RclReturnCode, RclrsError, SpinOptions,
};

use crate::config::Resolved;

/// One endpoint's slice of ROS: its own context and node, plus the thread that
/// pumps that node's callbacks.
///
/// A context per endpoint, rather than one per process, because an endpoint may
/// select its own `domain_id`, and because a route being torn down has to
/// release its ROS resources without disturbing any other route.
pub(crate) struct Ros2Runtime {
    /// The publisher and the subscription each hold their own reference to the
    /// node's handle. This one keeps the node in the ROS graph for as long as
    /// the endpoint lives, which is what makes it discoverable.
    node: Node,
    commands: Arc<ExecutorCommands>,
    /// Taken by whichever of [`Ros2Runtime::shutdown`] and `drop` runs first.
    spinner: Mutex<Option<JoinHandle<()>>>,
}

impl Ros2Runtime {
    /// Brings up the context and node, hands the node to `create` so the
    /// endpoint can build its publisher or subscription, and only then starts
    /// spinning.
    ///
    /// The order matters: nothing is delivered to a node that is not being
    /// spun, so creating the subscription first means no message published
    /// after this call can slip past a subscription that does not exist yet.
    pub(crate) fn start<T>(
        endpoint: &Resolved,
        create: impl FnOnce(&Node) -> Result<T, RclrsError>,
    ) -> Result<(Self, T), RclrsError> {
        let mut options = InitOptions::new();
        if let Some(domain_id) = endpoint.config.domain_id {
            options = options.with_domain_id(Some(domain_id));
        }
        let context = Context::from_env(options)?;
        let mut executor = context.create_basic_executor();
        let node = executor.create_node(
            endpoint
                .node
                .as_str()
                .namespace(endpoint.namespace.as_str()),
        )?;

        let primitive = create(&node)?;

        let commands = Arc::clone(executor.commands());
        // `spin` blocks its thread, and rclrs' basic executor is not a tokio
        // runtime, so it gets a thread of its own rather than a tokio task.
        // Spin errors are dropped deliberately: the route observes a broken
        // endpoint through its own receive/send failures, and a stopped
        // executor cannot report anything to anyone.
        let spinner = std::thread::spawn(move || {
            let _ = executor.spin(SpinOptions::default());
        });

        Ok((
            Self {
                node,
                commands,
                spinner: Mutex::new(Some(spinner)),
            },
            primitive,
        ))
    }

    pub(crate) fn node(&self) -> &Node {
        &self.node
    }

    /// Stops the executor and waits for its thread. Idempotent, so an explicit
    /// close followed by a drop does the work once.
    pub(crate) fn shutdown(&self) {
        self.commands.halt_spinning();
        if let Some(spinner) = lock(&self.spinner).take() {
            // A panic in the executor thread is already lost by the time we get
            // here; joining is about not leaving the thread running.
            let _ = spinner.join();
        }
    }
}

impl Drop for Ros2Runtime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The mutex only guards a `JoinHandle`, and a poisoned lock still holds a
/// perfectly good one, so recovering beats propagating a second panic.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Whether a ROS failure will fail identically however many times it is
/// retried.
///
/// This is the distinction the route acts on: a permanent error stops it, while
/// anything else is treated as a connection failure and retried on the
/// reconnect interval forever. A message type that is not installed, a package
/// that was never sourced and a name `rcl` rejects all belong in the first
/// group — waiting will not install a package.
pub(crate) fn is_permanent(error: &RclrsError) -> bool {
    match error {
        RclrsError::DynamicMessageError { .. } | RclrsError::StringContainsNul { .. } => true,
        RclrsError::RclError { code, .. } => matches!(
            code,
            RclReturnCode::InvalidArgument
                | RclReturnCode::TopicNameInvalid
                | RclReturnCode::NodeInvalidName
                | RclReturnCode::NodeInvalidNamespace
                | RclReturnCode::UnknownSubstitution
                | RclReturnCode::InvalidRemapRule
                | RclReturnCode::InvalidRosArgs
                | RclReturnCode::Unsupported
        ),
        _ => false,
    }
}
