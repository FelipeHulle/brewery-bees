# Data Pipeline - Brewery API

Este repositório contém um projeto em desenvolvimento para um pipeline de dados que consome dados da [Brewery API](https://www.openbrewerydb.org/). O objetivo é estruturar e processar os dados em um Data Lake utilizando o Apache Airflow. No entanto, a DAG e a estruturação do Data Lake ainda estão em progresso.

## Estrutura do Projeto

- **Pipeline de Dados:** Desenvolvido utilizando o Apache Airflow.
- **Orquestração:** Usamos o Astro CLI para gerenciar o ambiente do Airflow e os contêineres Docker.
- **Fonte de Dados:** [Brewery API](https://www.openbrewerydb.org/).
- **Estado Atual:** 
  - O ambiente do Airflow pode ser iniciado com o comando `astro dev start`.
  - A DAG e a estrutura completa do Data Lake ainda estão em desenvolvimento.

## Configuração do Ambiente

1. Certifique-se de ter o [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) instalado.
2. Clone este repositório:
   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd <NOME_DO_DIRETORIO>
   ```
3. Inicie o ambiente do Airflow:
   ```bash
   astro dev start
   ```
4. Acesse a interface web do Airflow em `http://localhost:8080` (usuário: `admin`, senha: `admin`).

## Testando o Código no Google Colab

Enquanto a DAG e o pipeline ainda estão sendo desenvolvidos, você pode testar a coleta e o processamento de dados diretamente no Google Colab. 

### Instruções:
1. Acesse o link abaixo para abrir o notebook no Colab:
   [Link para o Notebook](https://colab.research.google.com/drive/1CtZ6wf6s0MZYaPpnx8GHMwWw3oLMQ1T5?usp=sharing)
2. Siga as instruções no notebook para executar o código e verificar a coleta de dados da Brewery API.

## Próximos Passos

- [ ] Finalizar a implementação da DAG.
- [ ] Estruturar o Data Lake para armazenamento eficiente dos dados.
- [ ] Adicionar documentação completa das etapas do pipeline.
- [ ] Incluir testes automatizados para validar o funcionamento do pipeline.


Este repositório é mantido por Felipe Hulle.
