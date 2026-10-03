use crate::traits::{CustomEndpointFactory, CustomMiddlewareFactory};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock, RwLock};

static CUSTOM_ENDPOINT_REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn CustomEndpointFactory>>>> =
    OnceLock::new();
static CUSTOM_MIDDLEWARE_REGISTRY: OnceLock<
    RwLock<HashMap<String, Arc<dyn CustomMiddlewareFactory>>>,
> = OnceLock::new();

/// Registers an endpoint factory under `name` in the process-global registry.
///
/// Returns an error when that name is already registered (or the registry lock is poisoned).
pub fn register_endpoint_factory(
    name: &str,
    factory: Arc<dyn CustomEndpointFactory>,
) -> anyhow::Result<()> {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = registry
        .write()
        .map_err(|_| anyhow::anyhow!("custom endpoint registry lock poisoned"))?;
    if map.contains_key(name) {
        return Err(anyhow::anyhow!(
            "an endpoint factory named `{name}` is already registered"
        ));
    }
    map.insert(name.to_string(), factory);
    Ok(())
}

/// Returns the process-global endpoint factory registered under `name`, or `None` when no factory
/// has that name or the registry cannot be read.
pub fn get_endpoint_factory(name: &str) -> Option<Arc<dyn CustomEndpointFactory>> {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let map = registry.read().ok()?;
    map.get(name).cloned()
}

/// The configuration schema of every registered endpoint that declares one,
/// keyed by the name routes address it as.
///
/// Sorted, so a document built from it — a host's config schema, a UI's endpoint
/// list — is the same on every run. Both statically linked extensions and loaded
/// plugins answer here, so a host needs no separate path for either.
pub fn endpoint_config_schemas() -> BTreeMap<String, serde_json::Value> {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let Ok(map) = registry.read() else {
        return BTreeMap::new();
    };
    map.iter()
        .filter_map(|(name, factory)| {
            let schema = factory.config_schema()?;
            Some((
                name.clone(),
                crate::support::config_schema::flatten(&schema),
            ))
        })
        .collect()
}

/// The batch size the endpoint registered under `name` asks a host to use when
/// the user chose none: the `x-mqb-default-batch-size` integer at the top of its
/// configuration schema. Only a hint; a route's own `batch_size` always wins.
pub fn endpoint_default_batch_size(name: &str) -> Option<usize> {
    let schema = get_endpoint_factory(name)?.config_schema()?;
    let size = schema.get("x-mqb-default-batch-size")?.as_u64()?;
    usize::try_from(size).ok().filter(|size| *size > 0)
}

/// Removes the endpoint factory registered under `name`, freeing the name for
/// re-registration and dropping the registry's reference to the factory.
///
/// Returns `true` when a factory was removed, and `false` when no factory has
/// that name, nothing has ever been registered, or the registry cannot be
/// written. Consumers already built from the factory are unaffected.
pub fn unregister_endpoint_factory(name: &str) -> bool {
    if let Some(registry) = CUSTOM_ENDPOINT_REGISTRY.get() {
        if let Ok(mut factories) = registry.write() {
            return factories.remove(name).is_some();
        }
    }
    false
}

/// Registers a middleware factory under `name` in the process-global registry.
///
/// Returns an error when that name is already registered (or the registry lock is poisoned).
pub fn register_middleware_factory(
    name: &str,
    factory: Arc<dyn CustomMiddlewareFactory>,
) -> anyhow::Result<()> {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = registry
        .write()
        .map_err(|_| anyhow::anyhow!("middleware registry lock poisoned"))?;
    if map.contains_key(name) {
        return Err(anyhow::anyhow!(
            "a middleware factory named `{name}` is already registered"
        ));
    }
    map.insert(name.to_string(), factory);
    Ok(())
}

/// Returns the process-global middleware factory registered under `name`, or `None` when no factory
/// has that name or the registry cannot be read.
pub fn get_middleware_factory(name: &str) -> Option<Arc<dyn CustomMiddlewareFactory>> {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let map = registry.read().ok()?;
    map.get(name).cloned()
}

/// Every registered middleware, keyed by the name routes address it as, with
/// the configuration schema it declares (`None` when it declares none).
///
/// Unlike [`endpoint_config_schemas`], undeclared middlewares are listed too, so
/// a host can still offer them. Sorted, for the same reason.
pub fn middleware_config_schemas() -> BTreeMap<String, Option<serde_json::Value>> {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let Ok(map) = registry.read() else {
        return BTreeMap::new();
    };
    map.iter()
        .map(|(name, factory)| {
            let schema = factory.config_schema();
            let flat = schema.as_ref().map(crate::support::config_schema::flatten);
            (name.clone(), flat)
        })
        .collect()
}

/// Removes the middleware factory registered under `name`, freeing the name for
/// re-registration and dropping the registry's reference to the factory.
///
/// Returns `true` when a factory was removed, and `false` when no factory has
/// that name, nothing has ever been registered, or the registry cannot be
/// written. Middlewares already built from the factory are unaffected.
pub fn unregister_middleware_factory(name: &str) -> bool {
    if let Some(registry) = CUSTOM_MIDDLEWARE_REGISTRY.get() {
        if let Ok(mut factories) = registry.write() {
            return factories.remove(name).is_some();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct EndpointFactory;

    impl CustomEndpointFactory for EndpointFactory {}

    #[derive(Debug)]
    struct MiddlewareFactory;

    impl CustomMiddlewareFactory for MiddlewareFactory {}

    #[test]
    fn duplicate_endpoint_registration_is_rejected_without_replacing_the_factory() {
        let name = "extensions-test-duplicate-endpoint";
        let first: Arc<dyn CustomEndpointFactory> = Arc::new(EndpointFactory);

        register_endpoint_factory(name, Arc::clone(&first)).unwrap();
        let error = register_endpoint_factory(name, Arc::new(EndpointFactory)).unwrap_err();

        assert!(error.to_string().contains("already registered"));
        assert!(Arc::ptr_eq(&first, &get_endpoint_factory(name).unwrap()));
    }

    #[test]
    fn an_endpoint_states_its_default_batch_size_in_its_schema() {
        #[derive(Debug)]
        struct Bulk(serde_json::Value);
        impl CustomEndpointFactory for Bulk {
            fn config_schema(&self) -> Option<serde_json::Value> {
                Some(self.0.clone())
            }
        }
        let register = |name: &str, hint: serde_json::Value| {
            let schema = serde_json::json!({ "type": "object", "x-mqb-default-batch-size": hint });
            register_endpoint_factory(name, Arc::new(Bulk(schema))).unwrap();
        };
        register("extensions-test-batch-hint", 30_000.into());
        register("extensions-test-batch-zero", 0.into());
        register_endpoint_factory("extensions-test-batch-none", Arc::new(EndpointFactory)).unwrap();

        let hint = endpoint_default_batch_size;
        assert_eq!(hint("extensions-test-batch-hint"), Some(30_000));
        assert_eq!(hint("extensions-test-batch-zero"), None);
        assert_eq!(hint("extensions-test-batch-none"), None);
        assert_eq!(hint("extensions-test-batch-unregistered"), None);
    }

    #[test]
    fn duplicate_middleware_registration_is_rejected_without_replacing_the_factory() {
        let name = "extensions-test-duplicate-middleware";
        let first: Arc<dyn CustomMiddlewareFactory> = Arc::new(MiddlewareFactory);

        register_middleware_factory(name, Arc::clone(&first)).unwrap();
        let error = register_middleware_factory(name, Arc::new(MiddlewareFactory)).unwrap_err();

        assert!(error.to_string().contains("already registered"));
        assert!(Arc::ptr_eq(&first, &get_middleware_factory(name).unwrap()));
    }

    #[test]
    fn middleware_config_schemas_lists_declared_and_undeclared_middlewares() {
        #[derive(Debug)]
        struct Declaring;

        impl CustomMiddlewareFactory for Declaring {
            fn config_schema(&self) -> Option<serde_json::Value> {
                Some(serde_json::json!({ "type": "object" }))
            }
        }

        let declaring = "extensions-test-schema-declaring-middleware";
        let silent = "extensions-test-schema-silent-middleware";
        register_middleware_factory(declaring, Arc::new(Declaring)).unwrap();
        register_middleware_factory(silent, Arc::new(MiddlewareFactory)).unwrap();
        let schemas = middleware_config_schemas();
        unregister_middleware_factory(declaring);
        unregister_middleware_factory(silent);

        assert_eq!(
            schemas.get(declaring),
            Some(&Some(serde_json::json!({ "type": "object" })))
        );
        assert_eq!(schemas.get(silent), Some(&None));
    }

    #[test]
    fn unregistering_an_endpoint_frees_the_name_for_re_registration() {
        let name = "extensions-test-unregister-endpoint";
        register_endpoint_factory(name, Arc::new(EndpointFactory)).unwrap();

        assert!(unregister_endpoint_factory(name));
        assert!(get_endpoint_factory(name).is_none());
        assert!(!unregister_endpoint_factory(name));

        let second: Arc<dyn CustomEndpointFactory> = Arc::new(EndpointFactory);
        register_endpoint_factory(name, Arc::clone(&second)).unwrap();
        assert!(Arc::ptr_eq(&second, &get_endpoint_factory(name).unwrap()));
    }

    #[test]
    fn unregistering_a_middleware_frees_the_name_for_re_registration() {
        let name = "extensions-test-unregister-middleware";
        register_middleware_factory(name, Arc::new(MiddlewareFactory)).unwrap();

        assert!(unregister_middleware_factory(name));
        assert!(get_middleware_factory(name).is_none());
        assert!(!unregister_middleware_factory(name));

        let second: Arc<dyn CustomMiddlewareFactory> = Arc::new(MiddlewareFactory);
        register_middleware_factory(name, Arc::clone(&second)).unwrap();
        assert!(Arc::ptr_eq(&second, &get_middleware_factory(name).unwrap()));
    }
}
