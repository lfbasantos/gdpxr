# 2018 - gpdxr - Greenplum DDL Extractor & Replicator
PowerShell Tool for Greenplum Schema Replication - by luiz.filipe@remay.com.br / lfbasantos@gmail.com
#
Correções<br>
 - Correção no 'regexp' quando o nome do schema tem '_' underscores<br>
 - Finalizando os erros do script com 'throw' em vez de 'Break' pra possibilitar que um script pai faça try/catch<br>
#
Ferramenta PowerShell criada para permitir a troca de compressão de uma tabela de forma automatizada, mantendo uma cópia da tabela original em um schema auxiliar.
# Parametros
Os parametros devem ser fornecidos entre aspas duplas, para evitar problemas de interpretacao de espacos em branco no powershell.
<br>O usuario de acesso passado na variavel de conexao 'url' deve ter permissao de uso e leitura e alter table nas tabelas que serao trabalhadas.
<br>A versao atual do script faz o replace de compressao de uma tabela, preservando todas as caracteristicas da DDL original incluindo particao, e realizando a carga dos dados da tabela original para a tabela nova.
#
--pghome: informa o diretorio raiz dos clients greenplum
<br>Ex.: "--pghome=C:\Program Files (x86)\Greenplum\greenplum-clients-5.8.1\bin" 
#
--url: informa os dados de conexao com o banco de dados em formato JSON
<br>Ex.: "--url={'urls':[{'url':'1','user':'emc_luizs', 'pass':'hyr64ljt', 'host':'10.64.78.220', 'port':'5432', 'database':'sf_prd_re', 'schema':'procergs'}]}" 
#
--compression: informa a compressao desejada na tabela a ser substituida, json format
<br>Ex.: "--compression={'compressions':[{'compression':'to', 'level':'6', 'type':'zlib'}]}"
#
--table: informa schema.tabela que sera replicada
<br>Ex.: "--table=procergs.admft_spd_efd_arquivo_proc_cod_int_rmov_1017_5" 
#
--workarea: informa o schema de trabalho
<br>Ex.: "--workarea=procergs" 
#
--mode: informa o modo de operacao, sendo 1 para 'extract only', onde o script apenas extrai a ddl da tabela, e 2 para 'replace', onde o script faz a substituicao da tabela
<br>Ex.: "--mode=2" 
#
--ddlmode: informa o script se a extracao deve ser realizada com dados '=off' ou sem dados '=on'. Nao precisa ser informado caso o '--mode' seja igual a 2.
<br>Ex.: "--ddlmode=on" 
#
--debug: liga o modo de depuracao, onde o script ira informar passo a passo a execucao das atividades
<br>"--debug=1"
#
Ex.: 
<br>./gpdxr.v.2.ps1 "--pghome=C:\Program Files (x86)\Greenplum\greenplum-clients-5.8.1\bin" "--url={'urls':[{'url':'1','user':'user', 'pass':'pass', 'host':'10.64.78.220', 'port':'5432', 'database':'sf_prd_re', 'schema':'procergs'}]}" "--compression={'compressions':[{'compression':'to', 'level':'6', 'type':'zlib'}]}" "--table=procergs.admft_spd_efd_arquivo_proc_cod_int_rmov_1017_5" "--workarea=procergs" "--mode=2" "--ddlmode=on" "--debug=1" 
