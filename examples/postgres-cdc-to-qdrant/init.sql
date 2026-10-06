CREATE TABLE docs (
  id    bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  title text NOT NULL,
  body  text NOT NULL
);
-- TRUNCATE is left out: the route cannot clear the Qdrant collection.
CREATE PUBLICATION docs_pub FOR TABLE docs WITH (publish = 'insert, update, delete');
INSERT INTO docs (title, body) VALUES
  ('Refunds', 'Refunds are issued to the original payment method within 5 business days.'),
  ('Shipping', 'Orders ship from the Rotterdam warehouse and arrive in 2 to 4 days in the EU.'),
  ('Password reset', 'Use the "Forgot password" link on the sign-in page to get a reset email.');
