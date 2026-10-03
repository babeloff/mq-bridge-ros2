# mq-bridge-myendpoint

Myendpoint endpoint for [mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

```js
import { register } from "mq-bridge-myendpoint";

register();
```

Routes can then use `{ custom: { name: "myendpoint", config: { url: "..." } } }`.
