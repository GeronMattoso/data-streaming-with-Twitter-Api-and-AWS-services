
### 1 - Streaming de dados utilizando a API do twitter e serviços AWS.
### 2 - Análise de sentimento de tweets sobre o presidente do brasil utilizando pyspark para a classificação.
### 3 - importação dos dados para o power powerBI, e criação de dashboard com os resultados:

#### Tecnologias:
- AWS Glue
- AWS S3
- AWS Athena
- AWS IAM
- Twitter API v2
- Jupiter notebook
- Power BI

#### Linguagens:
- python + spark

#### Bibliotecas:
- tweepy
- boto3
- pyspark
- datatime
- json

#### SCRIPTS

O script RAW é responsável por extrair tweets da api do tweeter e jogar em um bucket do AWS S3, criando um arquivo json novo a cada 100 tweets coletados.

O script REF, carrega os dados do bucket RAW transformando-os em um dataframe do spark para realizar a análise de sentimento dos textos. Ele cria duas novas colunas sendo "simbolo" e "sentimento" para classificar os dados, salvando os resultados em outro bucket chamado ref.

Os scripts foram executados utilizando jobs do serviço AWS glue, porém é possivel a adaptação para a IDE VsCode ou semelhante, sendo nescessário a configuração dos dados de acesso a nuvem pelo AWS CLI.
