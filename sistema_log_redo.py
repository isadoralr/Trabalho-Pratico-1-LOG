import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
import re

# Configurações de conexão com o banco de dados
DB_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'db_log',
    'port': '5432'
}

def criar_tabelas():
    """Cria as tabelas necessárias e as triggers de log"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS clientes_em_memoria CASCADE;""")
    cur.execute("""DROP TABLE IF EXISTS log CASCADE;""")
    cur.execute("""
        CREATE UNLOGGED TABLE IF NOT EXISTS clientes_em_memoria (
            id SERIAL PRIMARY KEY,
            nome TEXT,
            saldo NUMERIC
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS log (
            log_id SERIAL PRIMARY KEY,
            transaction_id BIGINT,
            tipo TEXT, -- 'BEGIN', 'OP', 'COMMIT', 'ROLLBACK'
            operacao TEXT,
            id_cliente INT,
            nome_old TEXT,
            nome_new TEXT,
            saldo_old NUMERIC,
            saldo_new NUMERIC
        );
    """)
    # Função e trigger para INSERT
    cur.execute("""
    CREATE OR REPLACE FUNCTION log_insert_clientes()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO log (transaction_id, tipo, operacao, id_cliente, nome_new, saldo_new)
        VALUES (txid_current(), 'OP', 'INSERT', NEW.id, NEW.nome, NEW.saldo);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)
    
    cur.execute("""
    CREATE OR REPLACE TRIGGER trg_log_insert
    AFTER INSERT ON clientes_em_memoria
    FOR EACH ROW EXECUTE FUNCTION log_insert_clientes();
    """)

    # Função e trigger para UPDATE
    cur.execute("""
    CREATE OR REPLACE FUNCTION log_update_clientes()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO log (transaction_id, tipo, operacao, id_cliente, nome_old, nome_new, saldo_old, saldo_new)
        VALUES (txid_current(), 'OP', 'UPDATE', NEW.id, OLD.nome, NEW.nome, OLD.saldo, NEW.saldo);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)

    cur.execute("""
    CREATE OR REPLACE TRIGGER trg_log_update
    AFTER UPDATE ON clientes_em_memoria
    FOR EACH ROW EXECUTE FUNCTION log_update_clientes();
    """)

    # Função e trigger para DELETE
    cur.execute("""
    CREATE OR REPLACE FUNCTION log_delete_clientes()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO log (transaction_id, tipo, operacao, id_cliente, nome_old, saldo_old)
        VALUES (txid_current(), 'OP', 'DELETE', OLD.id, OLD.nome, OLD.saldo);
        RETURN OLD;
    END;
    $$ LANGUAGE plpgsql;
    """)

    cur.execute("""
    CREATE OR REPLACE TRIGGER trg_log_delete
    AFTER DELETE ON clientes_em_memoria
    FOR EACH ROW EXECUTE FUNCTION log_delete_clientes();
    """)

    conn.commit()
    cur.close()
    conn.close()

def executar_transacoes_do_arquivo(arquivo):
    """Executa as transações lidas do arquivo e registra no log apenas BEGIN, COMMIT e ROLLBACK. As operações são logadas automaticamente pelas triggers."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("\nExecutando transações do arquivo...")
    transaction_id = None
    transacao_atual = []
    with open(arquivo, 'r') as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            if linha == 'BEGIN;':
                # Se já existe uma transação aberta, finalize com ROLLBACK antes de iniciar nova
                if transaction_id is not None:
                    print(f"Transação {transaction_id} não finalizada explicitamente, realizando ROLLBACK.")
                    cur.execute("ROLLBACK;")
                    cur.execute("INSERT INTO log (transaction_id, tipo) VALUES (%s, 'ROLLBACK')", (transaction_id,))
                    transacao_atual = []
                    transaction_id = None
                cur.execute("BEGIN;")
                cur.execute("SELECT txid_current();")
                transaction_id = cur.fetchone()[0]
                # Loga o início da transação com o mesmo txid_current()
                cur.execute("INSERT INTO log (transaction_id, tipo) VALUES (%s, 'BEGIN')", (transaction_id,))
                transacao_atual = []
            elif linha in ['COMMIT;', 'END;', 'ROLLBACK;', 'commit;', 'end;', 'rollback;']:
                if transaction_id is None:
                    # BEGIN não foi encontrado antes, ignora
                    continue
                if linha.lower() in ['commit;', 'end;']:
                    try:
                        for cmd in transacao_atual:
                            cur.execute(cmd)
                        cur.execute("COMMIT;")
                        # Loga o commit
                        cur.execute("INSERT INTO log (transaction_id, tipo) VALUES (%s, 'COMMIT')", (transaction_id,))
                    except Exception as e:
                        print(f"Erro na transação: {e}")
                        cur.execute("ROLLBACK;")
                        # Loga o rollback
                        cur.execute("INSERT INTO log (transaction_id, tipo) VALUES (%s, 'ROLLBACK')", (transaction_id,))
                    transacao_atual = []
                    transaction_id = None
                else:
                    print("Transação com ROLLBACK - ignorando")
                    cur.execute("ROLLBACK;")
                    # Loga o rollback
                    cur.execute("INSERT INTO log (transaction_id, tipo) VALUES (%s, 'ROLLBACK')", (transaction_id,))
                    transacao_atual = []
                    transaction_id = None
                # Não incremente aqui, só no próximo BEGIN
            else:
                transacao_atual.append(linha)
    # Ao final do arquivo, se houver transação aberta, finalize com ROLLBACK (transação inacabada)
    if transaction_id is not None:
        print(f"Transação {transaction_id} não finalizada explicitamente, realizando ROLLBACK.")
        cur.execute("ROLLBACK;")
        cur.execute("INSERT INTO log (transaction_id, tipo) VALUES (%s, 'ROLLBACK')", (transaction_id,))
    cur.close()
    conn.close()

def simular_queda():
    """Simula a queda do sistema limpando a tabela em memória"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("TRUNCATE TABLE clientes_em_memoria RESTART IDENTITY;")
    
    cur.close()
    conn.close()

def realizar_redo():
    print("\n=== Iniciando processo de REDO ===")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM clientes_em_memoria;")
    cur.execute("ALTER SEQUENCE clientes_em_memoria_id_seq RESTART WITH 1;")
    # Descobre as transações commitadas
    cur.execute("SELECT DISTINCT transaction_id FROM log WHERE tipo = 'COMMIT'")
    commitadas = set(row[0] for row in cur.fetchall())
    cur.execute("SELECT DISTINCT transaction_id FROM log WHERE tipo = 'ROLLBACK'")
    rollbackadas = set(row[0] for row in cur.fetchall())
    transacoes_redo = commitadas - rollbackadas
    # Busca todas as operações do log ordenadas por log_id
    cur.execute("""
        SELECT transaction_id, operacao, id_cliente, nome_old, nome_new, saldo_old, saldo_new
        FROM log
        WHERE tipo = 'OP'
        ORDER BY log_id;
    """)
    operacoes = cur.fetchall()
    for tx_id, operacao, id_cliente, nome_old, nome_new, saldo_old, saldo_new in operacoes:
        if tx_id not in transacoes_redo:
            continue
        try:
            if operacao == 'INSERT':
                cur.execute("""
                    INSERT INTO clientes_em_memoria (id, nome, saldo) 
                    VALUES (%s, %s, %s);
                """, (id_cliente, nome_new, saldo_new))
            elif operacao == 'UPDATE':
                if nome_new != nome_old and nome_new is not None:
                    cur.execute("""
                        UPDATE clientes_em_memoria 
                        SET nome = %s 
                        WHERE id = %s;
                    """, (nome_new, id_cliente))
                if saldo_new != saldo_old and saldo_new is not None:
                    cur.execute("""
                        UPDATE clientes_em_memoria 
                        SET saldo = %s 
                        WHERE id = %s;
                    """, (saldo_new, id_cliente))
            elif operacao == 'DELETE':
                cur.execute("""
                    DELETE FROM clientes_em_memoria 
                    WHERE id = %s;
                """, (id_cliente,))
        except Exception:
            pass
    conn.commit()
    print_redo_report(cur, list(transacoes_redo))
    cur.close()
    conn.close()

def print_redo_report(cursor, committed_transactions):
    print(f"\n{'='*50}")
    print("RELATÓRIO DO PROCESSO DE REDO")
    print(f"{'='*50}")
    if not committed_transactions:
        print("Nenhuma transação foi recuperada.")
        return
    print(f"Transações recuperadas: {len(committed_transactions)}")
    for tx_id in committed_transactions:
        print(f"  - Transação {tx_id}: REDO realizado com sucesso")
    print(f"\n{'='*30}")
    print("ESTADO FINAL DA TABELA:")
    print(f"{'='*30}")
    cursor.execute("SELECT * FROM clientes_em_memoria ORDER BY id;")
    clientes = cursor.fetchall()
    if clientes:
        print(f"{'ID':<5} {'Nome':<15} {'Saldo':<10}")
        print("-" * 30)
        for cliente in clientes:
            print(f"{cliente[0]:<5} {cliente[1]:<15} {cliente[2]:<10}")
    else:
        print("Tabela vazia após recuperação.")
    print(f"\nTotal de registros recuperados: {len(clientes)}")

def mostrar_log():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("\nLOG DETALHADO DAS TRANSAÇÕES:")
    # Buscar todos os transaction_id distintos que estão no log
    cur.execute("select distinct transaction_id from log order by transaction_id;")
    tx_ids = [row[0] for row in cur.fetchall()]
    if not tx_ids:
        print("(O log está vazio!)")
        cur.close()
        conn.close()
        return
    for tx_id in tx_ids:
        print(f"\nTransação {tx_id}:")
        # Buscar todos os registros do log para o transaction_id
        cur.execute("select log_id, tipo, operacao, id_cliente, nome_old, nome_new, saldo_old, saldo_new from log where transaction_id = %s order by log_id;", (tx_id,))
        rows = cur.fetchall()
        status = 'NÃO FINALIZADA'
        for log_id, tipo, operacao, id_cliente, nome_old, nome_new, saldo_old, saldo_new in rows:
            if tipo == 'ROLLBACK':
                status = 'ROLLBACK'
            elif tipo == 'COMMIT' and status != 'ROLLBACK':
                status = 'COMMIT'
        print(f"  Status: {status}")
        for log_id, tipo, operacao, id_cliente, nome_old, nome_new, saldo_old, saldo_new in rows:
            if tipo == 'OP':
                print(f"    [{log_id}] {operacao} id={id_cliente} nome_old={nome_old} nome_new={nome_new} saldo_old={saldo_old} saldo_new={saldo_new}")
            elif tipo in ('BEGIN', 'COMMIT', 'ROLLBACK'):
                print(f"    [{log_id}] {tipo.lower()}")
    cur.close()
    conn.close()

def main():

    print("---- Criando tabelas e trigger ----")
    criar_tabelas()
    
    print("----Executando transações do arquivo ----")
    executar_transacoes_do_arquivo('transacoes.sql')
    mostrar_log()
    
    print("---- Simulando queda do sistema ----")
    simular_queda()
    
    print("---- Realizando REDO ----")
    realizar_redo()

if __name__ == "__main__":
    main() 