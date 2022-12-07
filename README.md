# Pipeline Para Tratamendo de Dados Brutos de Chuvas e Casos de Dengue por Estado Usando Apache Beam

>Projeto realizado juntamente com o Curso da Alura:  _Apache Beam: Data Pipeline com Python_

O seguinte projeto tem como finalidade o tratamento dos arquivos brutos com dados diários de chuva e de casos de dengue nos diversos estados do país (arquivos .csv e .txt) para que seja feita a correlação entre a quantidade de chuva e os casos de dengue mês a mês nos diferentes estados do país entre 2015 e 2019.

Através das pipelines são realizadas transformaçãos dos arquivos:
- **Dengue**:
    - Leitura dos dados;
    - Criação de chave que irá se relacionar com os dados de chuva (UF-ANO-MES);
    - Remoção de colunas extras;
    - Agrupamento e soma dos casos de dengue por estado e por mês.

- **Chuvas**:
    - Leitura dos dados;
    - Criação de chave que irá se relacionar com os dados de dengue (UF-ANO-MES);
    - Agrupamento e soma das chuvas acumuladas por estado e por mês.

- **Juntando os arquivos**:
    - Junção dos dados de chuvas e de casos de dengue e remoção de campos nulos (sem relacionamento);
    - Exportação para arquivo .csv pronto para análise.

## Arquivos do projeto

Encontram-se na pasta do projeto:
* Script Python da pipeline de transformação dos dados brutos;
* Arquivos com os dados brutos recebidos (.rar: necessário descompactar);
* Arquivo pronto para análise com resultado das transformações feitas.

*OBS: Note que na pasta dos dados brutos encontram-se também arquivos "sample" que foram utilizados dorante o desenvolvimento para agilizar os teste do script das pipelines.

-----------------------------
## Meta

Bruno Augusto --- [Linkedin](https://www.linkedin.com/in/brunoaugp/) --- brunoaugp@hotmail.com

Link do Curso:  https://cursos.alura.com.br/completeCourse/apache-beam-data-pipeline-python


<https://github.com/brunoaugp>


