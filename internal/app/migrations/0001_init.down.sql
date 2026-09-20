-- 0001_init.down.sql
-- Reversão completa do esquema inicial, em ordem inversa de criação.

DROP TABLE IF EXISTS inbox;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS wagering_transactions;
DROP TABLE IF EXISTS wallet_ledger;
DROP FUNCTION IF EXISTS prevent_ledger_modification();
DROP TABLE IF EXISTS wallets;
-- schema_version é mantida pelo runner de migrations, não pela migration.