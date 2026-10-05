CREATE TABLE orders (
  id     bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  sku    text NOT NULL,
  qty    int  NOT NULL,
  status text NOT NULL DEFAULT 'placed'
);

CREATE TABLE outbox (
  id           bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  aggregate_id text  NOT NULL,
  event_type   text  NOT NULL,
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- Only inserts are relayed, so the application can delete old outbox rows freely.
CREATE PUBLICATION outbox_pub FOR TABLE outbox WITH (publish = 'insert');
