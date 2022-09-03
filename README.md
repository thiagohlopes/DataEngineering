# DataEngineering

~.drawio é o modelo da arquitetura ao qual estava usando considerando um processo mais avançado de engenharia de dados
Modelo no qual estou usando uma arquitetura de deltawarehouse, com a recém lançado modelo delta 2.0 com CDF que permite pegar apenas as mudanças o que gera uma economia gigantesca quando se trabalha com big data, para este modelo usaria um esquema de batchIds, mas levaria um tempo extra que não teria necessidade para aplicação em questão

~.py são notebook para serem executados em databricks, recomendo que façam o download do arquivo ~.zip para que seja possivel realizar a importação de modo mais facil
o arquivo "1 - challenge summary" é um arquivo resumo que realiza o que foi solicitado no desafio

A orquestração eu usaria algum pipeline (Azure/GCP/Aws), neste caso a orquestração acaba se tornando mais simples e evitando custos extras, para garantir a execução dos processos eu usaria no lugar dos prints que estão ao longo do codigo uma função de enviar um alerta em algum workspace como teams, google chat, ..., pois caso ocorra um erro foi algo com a base de dados e deveria ser analisado para que não ocorra novamente, demonstrei isso no notebook "2 - csv_to_landing"

As fake credenciais estão em HardCode pois estava realizando testes no community databricks, contudo coloquei comentado o modo que pegaria as key do projeto
