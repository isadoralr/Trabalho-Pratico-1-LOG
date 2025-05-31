BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 1', 100.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 1;
END;
BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 2', 200.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 2;
END;
BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 3', 300.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 2;
END;

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 4', 400.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 3;

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 5', 600.00)

