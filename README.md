# Sistema de Log e REDO - BDII Trabalho Prático 1
## Aluna: Isadora Laís Ruschel

Sistema que demonstra o funcionamento de um mecanismo de REDO para recuperação de dados após falhas no sistema.

## Pré-requisitos

- Python 3.6 ou superior
- PostgreSQL 10 ou superior
- Biblioteca psycopg2

## Instalação

1. Instale as dependências necessárias:
- pip install psycopg2-binary

2. Configure o banco de dados PostgreSQL:
   - Crie um banco de dados chamado "db_log"
   - Verifique se as credenciais no arquivo "sistema_log_redo.py" estão corretas

## Como usar

1. Execute o sistema com Python:
- python sistema_log_redo.py

O script irá:
1. Criar as tabelas necessárias (tabela em memória e tabela de log)
2. Criar um trigger para registrar as operações no log
3. Simular algumas operações de INSERT e UPDATE
4. Simular uma queda do sistema
5. Realizar o processo de REDO para recuperar os dados

## Estrutura do Sistema

- "clientes_em_memoria": Tabela UNLOGGED que simula dados em memória
- "log": Tabela que armazena todas as operações realizadas
- Trigger automático que registra todas as operações na tabela de log

## Funcionamento

O sistema demonstra:
1. Como as operações são registradas automaticamente no log
2. Como o processo de REDO funciona após uma falha
3. Como os dados são recuperados usando as informações do log
4. Como transações não finalizadas explicitamente são tratadas automaticamente

## Detalhes do Controle de Transações

- Se uma transação for iniciada com "BEGIN;" mas não for finalizada com "END;", "COMMIT;" ou "ROLLBACK;", o sistema detecta e ela é encerrada automaticamente com "ROLLBACK".
- Sempre que um novo "BEGIN;" é encontrado sem que a transação anterior tenha sido finalizada, a transação anterior é encerrada automaticamente com um "ROLLBACK" e isso é registrado no log.
- Na impressão do log, todas as transações aparecem com o status correto:
    - "COMMIT" para transações finalizadas normalmente
    - "ROLLBACK" para transações finalizadas explicitamente ou automaticamente
    - "NÃO FINALIZADA" apenas se não houver nenhum encerramento registrado
- Isso garante que nenhuma transação fique "pendente" e que o log reflita fielmente o que ocorreu, inclusive em casos de falha ou erro de sintaxe no arquivo de transações.

## Observações

- Apenas operações commitadas são recuperadas
- O log mantém um registro temporal das operações
- O sistema usa triggers para garantir que todas as operações sejam registradas 