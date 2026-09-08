Carrinhos Abandonados

Projeto Python de engenharia de dados, com duas formas de execucao:

- PostgreSQL local via Docker, util para validar a modelagem SQL.
- PySpark no Databricks, util para rodar o processamento distribuido nos arquivos CSV/Parquet.

A modelagem segue a arquitetura da imagem: `tb_carts` centraliza as chaves para `tb_paymentmodes`, `tb_paymentinfos`, `tb_users`, `tb_addresses` via `p_paymentaddress`, `tb_cmssitelp` via `p_site`, e `tb_cartentries` aponta para carrinhos via `p_order`.

## Estrutura

- `docker-compose.yml`: sobe PostgreSQL 16 e o container do pipeline.
- `Dockerfile`: imagem Python do pipeline.
- `sql/postgres/01_schema.sql`: tabelas fisicas e relacionamentos.
- `sql/postgres/02_views.sql`: views analiticas `cart_items`, `cart_item_summary` e `cart_enriched`.
- `src/cantu_abandoned_carts/`: carga dos CSV/Parquet e geracao dos relatorios.
- `src/cantu_abandoned_carts/spark_pipeline.py`: versao PySpark para Databricks.
- `notebooks/databricks_pyspark.py`: notebook Databricks com widgets para `data_dir`, `output_dir` e `top_limit`.
- `sql/parte_1_respostas.sql`: respostas SQL da Parte 1.
- `doc/`: dados brutos da prova, mantidos localmente e nao versionados por tamanho/sensibilidade.
- `output/`: relatorios gerados.

## Executar no Databricks com PySpark

1. Crie um cluster Databricks com runtime que inclua PySpark.
2. Importe este repositorio Git no Databricks Repos ou envie o pacote para o workspace.
3. Envie os arquivos brutos da pasta local `doc/` para um caminho DBFS, por exemplo `dbfs:/FileStore/cantu/doc`. Esses dados nao ficam no GitHub por tamanho/sensibilidade.
4. Rode o notebook `notebooks/databricks_pyspark.py` ou execute o comando abaixo em um job/notebook com o pacote instalado:

```bash
python -m cantu_abandoned_carts.cli spark-run \
  --data-dir dbfs:/FileStore/cantu/doc \
  --output-dir dbfs:/FileStore/cantu/output
```

Os arquivos brutos da prova foram mantidos fora do Git. Eles permanecem na maquina local e devem ser carregados diretamente no Databricks/DBFS.

No Spark, cada saida e gravada como um diretorio com arquivo `part-*`, que e o formato padrao de escrita distribuida. Exemplos:

- `dbfs:/FileStore/cantu/output/top_produtos_abandonados.csv/`
- `dbfs:/FileStore/cantu/output/relatorio_diario.csv/`
- `dbfs:/FileStore/cantu/output/top_50_carrinhos.txt/`

## Executar com Docker

```bash
docker-compose up --build app
```

Esse comando cria o banco, recria o schema, carrega os dados em PostgreSQL e gera os arquivos em `output/`.

Para manter o banco rodando e executar etapas separadas:

```bash
docker-compose up -d postgres
docker-compose run --rm app python -m cantu_abandoned_carts.cli load --data-dir doc
docker-compose run --rm app python -m cantu_abandoned_carts.cli reports --output-dir output
docker-compose run --rm app python -m cantu_abandoned_carts.cli counts
```

Para limpar o volume do banco e reprocessar do zero:

```bash
docker-compose down -v
```

## Executar localmente sem Docker

Suba um PostgreSQL acessivel e configure as variaveis abaixo, ou use os defaults do compose (`localhost:5432`, banco `cantu`, usuario `cantu`, senha `cantu`).

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=cantu POSTGRES_USER=cantu POSTGRES_PASSWORD=cantu \
  .venv/bin/python -m cantu_abandoned_carts.cli run --data-dir doc --output-dir output
```

## Saidas geradas

- `output/top_produtos_abandonados.csv`: produtos com mais carrinhos abandonados.
- `output/top_duplas_produtos_abandonados.csv`: duplas de produtos que mais aparecem juntas em carrinhos abandonados.
- `output/produtos_com_aumento_abandono.csv`: produtos com aumento mes contra mes no abandono.
- `output/produtos_novos_primeiro_mes.csv`: primeiro mes observado de cada produto e quantidade de carrinhos nesse mes.
- `output/abandonos_por_estado.csv`: abandonos por UF seguindo `tb_carts.p_paymentaddress -> tb_addresses.pk -> tb_regions.pk`.
- `output/relatorio_produtos_mes.csv`: relatorio mensal por produto com carrinhos, itens e valor nao faturado.
- `output/relatorio_diario.csv`: relatorio diario com carrinhos, itens e valor nao faturado.
- `output/top_50_carrinhos.txt`: arquivo pipe-delimited dos 50 carrinhos de maior `tb_carts.p_totalprice`.

## Premissas

- Como a prova nao fornece tabela de pedidos concluidos, `tb_carts` foi tratado como o universo de carrinhos abandonados.
- Produtos sao identificados por `tb_cartentries.p_product`, pois nao ha tabela de produtos no pacote.
- Valor nao faturado usa `tb_carts.p_totalprice` nos relatorios por carrinho/data e `tb_cartentries.p_totalprice` nos relatorios por produto.
- O relacionamento de endereco segue a arquitetura solicitada na imagem: `tb_carts.p_paymentaddress` para `tb_addresses.pk`.
- As relacoes da imagem foram mantidas por colunas e `left join`, mas sem `FOREIGN KEY` fisica no PostgreSQL, porque os dados brutos possuem referencias orfas, por exemplo `tb_carts.p_paymentinfo` sem registro correspondente em `tb_paymentinfos`.
- A carga usa tabela temporaria por lote e `ON CONFLICT DO NOTHING` para ignorar PKs duplicadas nos arquivos brutos, preservando uma linha por chave no PostgreSQL.

## Testes

```bash
.venv/bin/pytest
```
