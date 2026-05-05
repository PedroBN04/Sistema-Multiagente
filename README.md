# AIOps: Sentinel NoSQL

Um sistema multiagente inteligente desenhado para revolucionar a Gestão de Incidentes (AIOps). Operando sobre um banco de dados NoSQL orientado a documentos, o Córtex-IT automatiza a triagem de *logs* caóticos, converte falhas de sistema em análises de impacto de negócio e despacha alertas estruturados para equipes de engenharia.

## A Principal Entrega de Valor: Ruído Zero e Economia Escalável

O maior gargalo na operação de *squads* de tecnologia é a "fadiga de alertas". Desenvolvedores recebem milhares de *logs* de erro brutos diários, perdendo tempo valioso para descobrir o que realmente afeta o usuário final. 

O Córtex-IT resolve este problema entregando **contexto acionável com eficiência de custos**. 

Em vez de forçar as IAs a conversarem entre si - o que consome uma quantidade massiva de *tokens* em APIs de LLM -, o sistema utiliza o banco de dados (`Agentes.db`) como o único meio de comunicação. O resultado é a entrega de um *card* estruturado (pronto para o Jira ou Slack) contendo apenas:
* O nível de severidade (ex: P1, P3).
* O impacto real no negócio (ex: "Carrinho de compras indisponível").
* O *squad* exato que deve atuar.

Tudo isso processando o texto pesado apenas uma vez e propagando JSONs leves pelas camadas de inteligência.

## Arquitetura do Sistema

O sistema é dividido em 3 agentes assíncronos e independentes. O fluxo de dados ocorre em camadas (leitura da esquerda para a direita, e de baixo para cima):
```text
[Camada 3] {Status: Analisado}  ----> Agente 3: Integrador ----> Gera Payload (Jira/Slack)
                                           ^
                                           | (Atualiza Status / Gera Resumo Executivo)
[Camada 2] {Status: Extraído}   ----> Agente 2: Arquiteto  ----> Define Prioridade/Impacto
                                           ^
                                           | (Grava JSON Estruturado)
[Camada 1] Log Bruto/TXT        ----> Agente 1: Coletor    ----> Extrai Padrão de Erro
