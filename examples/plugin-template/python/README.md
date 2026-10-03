# mq-bridge-myendpoint

Myendpoint endpoint for [mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

```python
import mq_bridge_myendpoint

mq_bridge_myendpoint.register()
```

Routes can then use `{"custom": {"name": "myendpoint", "config": {"url": "..."}}}`.
