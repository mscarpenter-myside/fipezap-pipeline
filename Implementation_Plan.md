Extração de GeoJSON das Capitais (IBGE)
Este plano detalha as alternativas viáveis para extrair a malha de bairros de todas as capitais brasileiras, convertendo a base oficial do Instituto Brasileiro de Geografia e Estatística (IBGE) para o formato GeoJSON, ideal para web e compatível com nosso pipeline (Flourish).

Contexto da Fonte (IBGE)
O IBGE não fornece nativamente arquivos prontas em 
.geojson
 separados por capital. A forma oficial de obter essas geometrias é através do portal de Geociências > Organização do Território > Malhas Territoriais > Malha de Setores Censitários (Bairros). Os dados são fornecidos em formatos GIS densos: Shapefile (.shp) ou GeoPackage (.gpkg), divididos por Estado (UF) ou em um único arquivo gigantesco do Brasil inteiro.

Soluções / Alternativas de Extração
Abaixo proponho duas abordagens. A Opção A foca no uso do QGIS (conforme solicitado para considerarmos), permitindo controle visual do processo. A Opção B foca em automação escalável por código.

Opção A: Processamento Manual e Visual (QGIS)
Ideal se você quiser visualizar os mapas antes, conferir os limites dos municípios e tratar inconsistências na mão de forma visual.

Download da Fonte:
Acessar o site do IBGE e baixar o "Arquivo geoespacial de Bairros – Brasil" em formato Shapefile (.shp).
Importação no QGIS:
Abrir o QGIS e arrastar as camadas do Shapefile extraído.
A tabela de atributos do IBGE geralmente possui colunas como NM_MUN (Nome do Município) e NM_BAIRRO (Nome do Bairro).
Filtro (Query Builder):
Aplicar um filtro na camada (Layer Properties > Source > Query Builder) informando o nome das 22 capitais alvo do FipeZAP.
Exemplo de SQL no QGIS: "NM_MUN" IN ('São Paulo', 'Rio de Janeiro', 'Belo Horizonte', ...)
Desmembramento e Exportação (Split Vector Layer):
Usar a ferramenta nativa do QGIS Split Vector Layer indicando o campo NM_MUN.
O QGIS vai gerar um arquivo separado para cada capital.
Durante a exportação, selecionamos o formato GeoJSON, CRS EPSG:4326 (WGS84 - Padrão Web), e selecionamos para exportar apenas a coluna do nome do bairro (descartando os outros metadados inúteis do IBGE).
Prós: Feedback visual imediato; fácil de validar se o limite da cidade está correto; permite fechar "buracos" manualmente se o IBGE falhar.
Contras: Processo 100% manual e repetitivo. Demorado para refazer caso saia uma versão nova da malha do IBGE no próximo ano.
Opção B: Processamento Automatizado (Script Python com GeoPandas)
Ideal para o contexto de um pipeline de Engenharia de Dados reprodutível.

Download da Fonte: Igual à Opção A (baixar um único arquivo do Brasil inteiro ou por UF).
Script de Extração (extract_ibge_capitals.py):
Criar um script Python utilizando a biblioteca geopandas (que você já tem no ambiente).
O script lê o Shapefile bruto.
Filtra em memória o DataFrame geoespacial baseado em uma lista predefinida das 22 capitais FipeZAP.
Faz um loop salvando diretamente na pasta data/geojsons_brutos/[NOME_DA_CAPITAL].geojson.
Limpeza Automática: No mesmo script, forçamos manter apenas a coluna do nome do bairro e a geometria, removendo peso excedente de imediato.
Prós: Reprodutível em 1 segundo (Run Script); zero cliques; padroniza o output eliminando chance de erro humano na exportação.
Contras: Zero feedback visual no momento da extração (necessita abrir os arquivos finais em sites como geojson.io para validar).
User Review Required
IMPORTANT

Qual abordagem você prefere seguir?

Opção A (QGIS): Você (ou nós juntos) faremos o processo através da interface do software QGIS.
Opção B (Automação Python): Desenvolvo um script em Python (GeoPandas) para ler o banco de dados do IBGE, fatiar e cuspir as 22 capitais perfeitas na pasta.
Abordagem Híbrida: Usamos um script (Opção B) para ler o dado do IBGE e extrair os arquivos e, em seguida, você usa o QGIS apenas para "dar um tapa"/conferir os geojsons resultantes.
Assim que definirmos o norte, seguimos para a execução!