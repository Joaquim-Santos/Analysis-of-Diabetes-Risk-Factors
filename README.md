Execução do projeto:

Instalar todas as dependências do Python listadas no requirements.txt.
Utilizar Jupyter Notebook ou semelhante, para execução dos arquivos contidos no diretório de notebooks. Esses contém o passo a passo de toda a solução, para uma experiência mais interativa.
Será necessário ter definida uma instância do MinIO, tendo sua conexão configurada nos pontos do código (notebooks e DAGs). Recomenda-se o uso em docker, com mapeamento de volumes para o diretório datalake.
Será necessário ter definida uma instância do Airflow, a fim de executar as DAGs do diretório airflow, as quais automatizaram a execução de todo pipeline para geração da solução. Além disso, deve-se definir as variáveis de ambiente na instância do airflow, que são usadas no código das DAGs. Recomenda-se o uso em docker, com mapeamento de volumes para o diretório airflow/dags.


1.	Entendimento do negócio

O problema ser resolvido se encontra no domínio de saúde, com foco no diagnóstico da doença diabetes. O objetivo consiste em, a partir de características de uma pessoa, como comportamentos de risco e cuidados relacionados à saúde, além de condições crônicas de saúde, detectar quais possuem a doença em questão. Além disso, busca-se determinar quais os principais fatores de risco para diabetes e pré-diabetes.

2.	 Escopo

A fonte de dados consiste em datasets obtidos do Kaggle: https://www.kaggle.com/alexteboul/diabetes-health-indicators-dataset

Dado o contexto, busca-se gerar uma solução baseada em dados que permita antecipar o diagnóstico da doença, a fim de reduzir o massivo custo desse processo na economia do país. Para tanto, será construído um modelo preditivo para o risco de diabetes, no intuito de possibilitar um diagnóstico precoce, o qual pode levar a mudanças mais saudáveis de estilo de vida, assim como tratamentos mais eficazes.
 
Nesse projeto, cobrindo todas as etapas de um projeto real de Data Science, é possível contribuir para resolver o problema de como fazer uso dos dados para auxiliar órgãos de saúde, respondendo às questões:

•	Quais fatores de risco são mais preditivos de risco de diabetes?
•	Com base nos dados analisados, é possível fornecer previsões precisas de se um indivíduo possui diabetes?
•	Podemos usar um subconjunto dos fatores de risco para prever com precisão se um indivíduo tem diabetes?

Busca-se fornecer a solução para consumo através de Dashboards e uma API, para que os Órgãos responsáveis possam verificar se um indivíduo possui o risco de diabetes, com base em atributos como hábitos de cuidado com a saúde, status socioeconômico e condições de saúde. Além disso, poderá ser visualizada a relação dessas características com a ocorrência da doença, por meio de gráficos dinâmicos.

3. Solução Proposta

Para resolver esse problema foi construído uma solução completa para armazenamento, gestão e automatização de fluxos de dados utilizando tecnologias como Apache Airflow, Docker e Minio, além de explorar uma suíte poderosa de tecnologias para trabalhar com Análise de Dados e Machine Learning, tais como: Pandas, Scikit-learn, Pycaret,
SweetViz, Streamlit e Dash.

Através desse projeto foi possível praticar e implementar conceitos importantes da Ciência e Engenharia de Dados e propor uma solução para um problema de alta imortância para Órgãos de Saúde, a fim de prover melhores tratamentos de saúde para a população, por meio da Análise de Dados.
